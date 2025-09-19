#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import requests

from .conf import is_component_enabled
from .instrumentation.http import TracingHTTPAdapter

logger = logging.getLogger(__name__)


class HttpClient:
    """
    HTTP client with built-in tracing support.
    Drop-in replacement for requests with automatic tracing.
    """

    def __init__(self, base_url: str = None, headers: dict = None, timeout: int = 10, retry: int = 0):
        self.base_url = base_url
        self.default_headers = headers or {}
        self.timeout = timeout
        self.retry = retry
        self.session = requests.Session()

        # Install tracing adapter
        if is_component_enabled("http_requests"):
            adapter = TracingHTTPAdapter(max_retries=self.retry)
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

    def post(self, url: str, data=None, headers=None, **kwargs):
        """Make POST request with tracing."""
        return self.session.post(
            self._prepare_url(url),
            data=data,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def put(self, url: str, data=None, headers=None, **kwargs):
        """Make PUT request with tracing."""
        return self.session.put(
            self._prepare_url(url),
            data=data,
            headers=self._prepare_headers(headers),
            timeout=kwargs.get('timeout', self.timeout),
            **kwargs
        )

    def patch(self, url: str, data=None, headers=None, **kwargs):
        """Make PATCH request with tracing."""
        return self.session.patch(
            self._prepare_url(url),
            data=data,
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


http_client = HttpClient()
