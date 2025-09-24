from django.apps import AppConfig
from django.conf import settings
from .instrumentation import *
from jaegertrace.conf import is_component_enabled


class LoggerConfig(AppConfig):
    name = 'jaegertrace'

    def ready(self):
        if not hasattr(settings, 'TRACING_CONFIG'):
            return
        if is_component_enabled('database'):
            DatabaseInstrumentation.install()
        if is_component_enabled('redis'):
            RedisInstrumentation.install()
        if is_component_enabled('celery'):
            HTTPInstrumentation.install()
