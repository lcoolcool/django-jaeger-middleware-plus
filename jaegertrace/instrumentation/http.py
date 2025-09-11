"""
HTTP client instrumentation for outgoing requests.
Automatically traces HTTP requests made using the requests library.
"""
import logging
import time

import requests
from opentracing import Format
from opentracing.ext import tags
from requests.adapters import HTTPAdapter
from urllib3.util import parse_url

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span

logger = logging.getLogger(__name__)


class TracingHTTPAdapter(HTTPAdapter):
    """HTTP adapter that adds tracing to requests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tracer = initialize_global_tracer()
        self._config = get_tracing_config().get("http_requests", {})

    def send(self, request, **kwargs):
        """Send request with tracing."""
        if not is_component_enabled("http_requests"):
            return super().send(request, **kwargs)

        span = self._create_span(request)

        try:
            # Inject tracing headers
            self._inject_headers(request, span)

            # Send request
            start_time = time.time()
            response = super().send(request, **kwargs)
            duration = (time.time() - start_time) * 1000  # Convert to milliseconds

            # Add response information to span
            if response:
                span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)
                span.set_tag("http.response_size", len(response.content))
                span.set_tag("http.duration_ms", duration)

                if response.status_code >= 400:
                    span.set_tag(tags.ERROR, True)

            return response

        except Exception as e:
            span.set_tag(tags.ERROR, True)
            span.log_kv({
                "event": "error",
                "error.kind": e.__class__.__name__,
                "error.object": str(e),
                "message": str(e),
            })
            raise
        finally:
            span.finish()

    def _create_span(self, request):
        """Create tracing span for HTTP request."""
        parent_span = get_current_span()
        parsed_url = parse_url(request.url)

        operation_name = f"{request.method} {parsed_url.path or '/'}"

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_span
        )

        # Set standard HTTP tags
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        span.set_tag(tags.HTTP_URL, request.url)
        span.set_tag(tags.HTTP_METHOD, request.method)
        span.set_tag(tags.COMPONENT, "requests")

        if parsed_url.host:
            span.set_tag(tags.PEER_HOST_IPV4, parsed_url.host)
        if parsed_url.port:
            span.set_tag(tags.PEER_PORT, parsed_url.port)

        # Add request size
        if request.body:
            span.set_tag("http.request_size", len(request.body))

        return span

    def _inject_headers(self, request, span):
        """Inject tracing headers into request."""
        if not self._config.get("trace_headers", True):
            return

        carrier = {}
        try:
            self._tracer.inject(
                span_context=span.context,
                format=Format.HTTP_HEADERS,
                carrier=carrier
            )

            # Add tracing headers to request
            for key, value in carrier.items():
                request.headers[key] = value

        except Exception as e:
            logger.debug(f"Failed to inject tracing headers: {e}")


class HTTPInstrumentation:
    """HTTP client instrumentation manager."""

    @staticmethod
    def install():
        """Install HTTP client instrumentation."""
        if not is_component_enabled("http_requests"):
            return

        # Monkey patch requests Session to use tracing adapter
        original_session_init = requests.Session.__init__

        def traced_session_init(session_self):
            original_session_init(session_self)
            # Add tracing adapter
            adapter = TracingHTTPAdapter()
            session_self.mount('http://', adapter)
            session_self.mount('https://', adapter)

        requests.Session.__init__ = traced_session_init
        logger.info("HTTP client instrumentation installed")

    @staticmethod
    def uninstall():
        """Uninstall HTTP client instrumentation (not implemented)."""
        # This is complex to implement properly
        logger.warning("HTTP client instrumentation uninstall not implemented")


class TracedHTTPClient:
    """
    HTTP client with built-in tracing support.
    Drop-in replacement for requests with automatic tracing.
    """

    def __init__(self, base_url: str = None, headers: dict = None, timeout: int = 30):
        self.base_url = base_url
        self.default_headers = headers or {}
        self.timeout = timeout
        self.session = requests.Session()

        # Install tracing adapter
        if is_component_enabled("http_requests"):
            adapter = TracingHTTPAdapter()
            self.session.mount('http://', adapter)
            self.session.mount('https://', adapter)

    def _prepare_url(self, url: str) -> str:
        """Prepare URL with base URL if needed."""
        if self.base_url and not url.startswith(('http://', 'https://')):
            return f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        return url

    def _prepare_headers(self, headers: dict = None) -> dict:
        """Merge default headers with request headers."""
        merged_headers = self.default_headers.copy()
        if headers:
            merged_headers.update(headers)
        return merged_headers

    def get(self, url: str, params=None, headers=None, **kwargs):
        """Make GET request with tracing."""
        return self.session.get(
            self._prepare_url(url),
            params=params,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def post(self, url: str, data=None, json=None, headers=None, **kwargs):
        """Make POST request with tracing."""
        return self.session.post(
            self._prepare_url(url),
            data=data,
            json=json,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def put(self, url: str, data=None, json=None, headers=None, **kwargs):
        """Make PUT request with tracing."""
        return self.session.put(
            self._prepare_url(url),
            data=data,
            json=json,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def patch(self, url: str, data=None, json=None, headers=None, **kwargs):
        """Make PATCH request with tracing."""
        return self.session.patch(
            self._prepare_url(url),
            data=data,
            json=json,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def delete(self, url: str, headers=None, **kwargs):
        """Make DELETE request with tracing."""
        return self.session.delete(
            self._prepare_url(url),
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )
