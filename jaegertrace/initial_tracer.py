#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tracer initialization and management.
Handles Jaeger tracer setup and global tracer access.
"""

import logging

import opentracing
from jaeger_client import Config

from .conf import get_tracer_config, get_service_name
from .exceptions import TracingInitializationError

logger = logging.getLogger(__name__)


def initialize_global_tracer():
    _config = Config(
        config=get_tracer_config(),
        service_name=get_service_name(),
        validate=True,
    )
    if _config.initialized():
        tracer = opentracing.tracer
    else:
        try:
            # use uwsgi
            # multi-processing, fork(2) issues see:
            # https://github.com/jaegertracing/jaeger-client-python/issues/60
            # https://github.com/jaegertracing/jaeger-client-python/issues/31
            from uwsgidecorators import postfork

            @postfork
            def post_fork_initialize_jaeger():
                _config.initialize_tracer()

            tracer = opentracing.tracer
        except ImportError:
            # use gunicorn etc.
            tracer = _config.initialize_tracer()
        finally:
            if not _config.initialized():
                raise TracingInitializationError(
                    'Failed to initialize Jaeger tracer.'
                )
            logger.info(f'Jaeger tracer initialized for service: {get_service_name()}')
    return tracer
