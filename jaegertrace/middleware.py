#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import urllib

from django.utils.functional import cached_property
from opentracing import Format
from opentracing.ext import tags
from .conf import *
from .request_context import span_in_context, span_out_context, get_current_span

logger = logging.getLogger(__name__)


class TraceMiddleware:
    """"use jaeger_client realizing tracing"""

    def __init__(self, get_response=None):
        self.get_response = get_response

        # Initialize tracer
        self.tracer = self._tracer

    @cached_property
    def _tracer(self):
        from .initial_tracer import initialize_global_tracer
        return initialize_global_tracer()

    def __call__(self, request):
        if self._ignore_request(request):
            return self.get_response(request)

        self._parse_wsgi_headers(request)
        self.full_url(request)

        try:
            parent_ctx = self.tracer.extract(
                Format.HTTP_HEADERS,
                carrier=request.headers
            )
        except Exception as e:
            logger.exception(f'Failed to extract parent context:{e}')
            parent_ctx = None

        span = self._gen_span(request, parent_ctx)
        span_in_context(span)

        # # Add tracing headers to request
        carrier = {}
        try:
            self._tracer.inject(
                span_context=span.context,
                format=Format.HTTP_HEADERS,
                carrier=carrier
            )
            for key, value in carrier.items():
                request.headers[key] = value
        except Exception as e:
            logger.debug(f"Failed to inject tracing headers: {e}")

        response = self.get_response(request)
        trace_id_header = get_tracer_config().get("trace_id_header", "trace-id")
        response[trace_id_header] = span.trace_id

        try:
            span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)
        except Exception as e:
            logger.exception(f'Error setting response tags for tracing:{e}')
        finally:
            span.finish()
            span_out_context()

        return response

    @staticmethod
    def _ignore_request(request) -> bool:
        """
        Check if the request should be ignored based on configuration.
        :param: request:
        :return: True if request should be ignored, False otherwise.
        """
        ignore_urls = get_tracing_config().get("http_requests", {}).get("ignore_urls", [])
        request_path = request.path_info

        return any(
            ignore_url in request_path
            for ignore_url in ignore_urls
        )

    @staticmethod
    def _parse_wsgi_headers(request):
        """
        HTTP headers are presented in WSGI environment with 'HTTP_' prefix.
        This method finds those headers, removes the prefix, converts
        underscores to dashes, and converts to lower case.
        :param request:
        :return: returns a dictionary of headers
        """
        prefix = 'HTTP_'
        p_len = len(prefix)
        # use .items() despite suspected memory pressure bc GC occasionally
        # collects wsgi_environ.iteritems() during iteration.
        headers = {
            key[p_len:].replace('_', '-').lower():
                val for (key, val) in request.environ.items()
            if key.startswith(prefix)}
        setattr(request, 'headers', headers)

    @staticmethod
    def full_url(request):
        """
        Build the full URL from WSGI environ variables.
        Taken from:
        http://legacy.python.org/dev/peps/pep-3333/#url-reconstruction
        :return: Reconstructed URL from WSGI environment.
        """
        environ = request.environ
        url = environ['wsgi.url_scheme'] + '://'

        if environ.get('HTTP_HOST'):
            url += environ['HTTP_HOST']
        else:
            url += environ['SERVER_NAME']

            if environ['wsgi.url_scheme'] == 'https':
                if environ['SERVER_PORT'] != '443':
                    url += ':' + environ['SERVER_PORT']
            else:
                if environ['SERVER_PORT'] != '80':
                    url += ':' + environ['SERVER_PORT']

        url += urllib.parse.quote(environ.get('SCRIPT_NAME', ''))
        url += urllib.parse.quote(environ.get('PATH_INFO', ''))
        if environ.get('QUERY_STRING'):
            url += '?' + environ['QUERY_STRING']
        setattr(request, 'full_url', url)

    def _gen_span(self, request, parent_ctx):
        """
        Create a span for the request.
        :param request:
        :param parent_ctx:
        :return:
        """
        operation_name = '{} {}'.format(request.method, request.path)

        span_tags = {
            tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
            tags.HTTP_URL: request.full_url,
            tags.HTTP_METHOD: request.method,
            tags.COMPONENT: get_service_name()
        }

        remote_ip = request.environ.get('REMOTE_ADDR')
        if remote_ip:
            span_tags[tags.PEER_HOST_IPV4] = remote_ip

        remote_port = request.environ.get('REMOTE_PORT')
        if remote_port:
            span_tags[tags.PEER_PORT] = remote_port

        user_agent = request.META.get("HTTP_USER_AGENT")
        if user_agent:
            max_length = get_tracing_config().get("http_requests", {}).get("max_tag_value_length", 1024)
            span_tags["http.user_agent"] = user_agent[:max_length]

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_ctx,
            tags=span_tags)

        return span


def _tracing_injection(func):
    def _call(*args, **kwargs):
        request = args[1]

        try:
            span = get_current_span()
            if span:
                trace_id_header = get_tracer_config().get("trace_id_header", "trace-id")
                request.headers[trace_id_header] = span.trace_id
        except Exception:
            pass
        return func(*args, **kwargs)  # actual call

    return _call


sys = __import__("sys")
session = sys.modules['requests.sessions']
session.Session.prepare_request = _tracing_injection(session.Session.prepare_request)
