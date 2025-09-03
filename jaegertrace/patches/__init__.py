from .database import patch_database
from .redis import patch_redis
from .celery import patch_celery

__all__ = ['apply_all_patches', 'patch_database', 'patch_redis', 'patch_celery']


def apply_all_patches():
    patch_database()
    patch_redis()
    patch_celery()
