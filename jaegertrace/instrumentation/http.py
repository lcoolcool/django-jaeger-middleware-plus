"""
HTTP client instrumentation for outgoing requests.
Automatically traces HTTP requests made using the requests library.
"""
import logging
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

    def _should_ignore_tracing(self, request) -> bool:
        """Check if request should be traced."""
        ignore_urls = self._config.get("ignore_urls", [])
        request_url = request.url

        return any(
            ignore_url in request_url
            for ignore_url in ignore_urls
        )

    def send(self, request, **kwargs):
        """Send request with tracing."""
        if self._should_ignore_tracing(request):
            return super().send(request, **kwargs)

        span = self._create_span(request)

        try:
            self._inject_headers(request, span)
            response = super().send(request, **kwargs)

            # Add response information to span
            if response:
                span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)

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

        return span

    def _inject_headers(self, request, span):
        """Inject tracing headers into request."""
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
