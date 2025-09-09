#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import urllib


from django.utils.deprecation import MiddlewareMixin
from opentracing import Format
from opentracing.ext import tags
from .conf import *
from request_context import get_current_span, span_in_context, span_out_context

logger = logging.getLogger(__name__)


class TraceMiddleware(MiddlewareMixin):
    """"use jaeger_client realizing tracing"""

    def __init__(self, get_response=None):
        self.get_response = get_response
        self._tracer = None
        self._http_config = get_tracing_config().get("http_requests", {})
        self._tracer_config = get_tracer_config()

        # Initialize tracer if HTTP tracing is enabled
        if is_component_enabled("http_requests"):
            from initial_tracer import initialize_global_tracer
            self._tracer = initialize_global_tracer()

    def _should_ignore_request(self, request) -> bool:
        """
        Check if the request should be ignored based on configuration.
        :param: request:
        :return: True if request should be ignored, False otherwise.
        """
        if not is_component_enabled("http_requests"):
            return True

        ignore_urls = self._http_config.get("ignore_urls", [])
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

    def _inject_headers(self, request, span):
        """Inject tracing headers into request."""
        if not self._http_config.get("trace_headers", True):
            return

        carrier = request.META
        try:
            self._tracer.inject(
                span_context=span.context,
                format=Format.HTTP_HEADERS,
                carrier=carrier
            )
        except Exception as e:
            logger.debug(f"Failed to inject tracing headers: {e}")

    def process_request(self, request):
        """
        Process incoming HTTP request and start tracing span.
        :param request:
        :return:
        """
        if self._should_ignore_request(request):
            return

        # Parse headers and build URL
        self._parse_wsgi_headers(request)
        self.full_url(request)

        # Extract parent span context from request headers
        try:
            parent_ctx = self._tracer.extract(
                Format.HTTP_HEADERS,
                carrier=request.headers
            )
        except Exception as e:
            logger.exception(f'Failed to extract parent context:{e}')
            parent_ctx = None

        operation_name = '{} {}'.format(request.method, request.path)

        # Create standard tags for the request span
        span_tags = {
            tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
            tags.HTTP_URL: request.full_url,
            tags.HTTP_METHOD: request.method,
            tags.COMPONENT: get_service_name() or "django"
        }

        remote_ip = request.environ.get('REMOTE_ADDR')
        if remote_ip:
            span_tags[tags.PEER_HOST_IPV4] = remote_ip

        remote_port = request.environ.get('REMOTE_PORT')
        if remote_port:
            span_tags[tags.PEER_PORT] = remote_port

        user_agent = request.META.get("HTTP_USER_AGENT")
        if user_agent:
            max_length = self._http_config.get("max_tag_value_length", 1024)
            span_tags["http.user_agent"] = user_agent[:max_length]

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_ctx,
            tags=span_tags)

        # # Add tracing headers to request
        if self._http_config.get("trace_headers", True):
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

        # Store span in context
        span_in_context(span)

    def process_response(self, request, response):
        span = get_current_span()

        # Add trace-id header to response
        trace_id_header = self._tracer_config.get("trace_id_header", "trace-id")
        response[trace_id_header] = request.headers.get(trace_id_header, "")

        if not span:
            # logger.exception('Can not get valid span for tracing.')
            return response

        try:
            span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)
        except Exception as e:
            logger.exception(f'Error setting response tags for tracing:{e}')
        finally:
            span.finish()
            span_out_context()

        return response
