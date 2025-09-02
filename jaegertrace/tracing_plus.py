from conf import TRACING_PLUS_CONFIG


if TRACING_PLUS_CONFIG.get("sql"):
    def sql_as_name(func, *args, **kwargs):
        # sql = str(args[1])
        # if len(sql) < 60:
        #     return sql
        # try:
        #     if sql.startswith("UPDATE"):
        #         return sql[0:sql.index("` ") + 1]
        #     if sql.startswith("INSERT"):
        #         return sql[0:sql.index("` ") + 1]
        #     if sql.startswith("DELETE"):
        #         return sql[0:sql.index("` ") + 1]
        #     if sql.startswith("SELECT"):
        #         return sql[0:sql.index("`.") + 1]
        # except:
        #     return sql[:60]
        return "DB_QUERY"


    def span_processor(func, span, *args, **kwargs):
        span.set_tag(tags.DATABASE_STATEMENT, args[1])
        span.set_tag(tags.DATABASE_INSTANCE, TRACER_SERVICE_NAME)
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)


    from MySQLdb.cursors import Cursor

    try:
        # patch MySQLdb
        from MySQLdb.cursors import Cursor
    except ImportError:
        # path pymysql
        from pymysql.cursors import Cursor
    Cursor.execute = tracing_enabled(Cursor.execute, sql_as_name, span_processor)

if TRACING_CONFIG.get("mns"):
    def mns_span_processor(func, span, *args, **kwargs):
        # message tag
        span.set_tag(tags.COMPONENT, kwargs.get("tag", ""))

    from djtool.mq.client import mns_client
    mns_client.publish_message = tracing_enabled(mns_client.publish_message)

if TRACING_CONFIG.get("celery"):
    def celery_name(func, *args, **kwargs):
        return "CELERY"

    def celery_span_processor(func, span, *args, **kwargs):
        # task name
        span.set_tag(tags.COMPONENT, getattr(args[0], "name", ""))
    from celery.app.task import Task
    Task.apply_async = tracing_enabled(Task.apply_async, name_generator=celery_name, span_processor=celery_span_processor)

if TRACING_CONFIG.get("mongo"):
    def mongo_name(func, *args, **kwargs):
        return "MONGO"

    from pymongo.collection import Cursor as MongoCursor
    MongoCursor._refresh = tracing_enabled(MongoCursor._refresh, name_generator=mongo_name)


if TRACING_CONFIG.get("redis"):
    def redis_name(func, *args, **kwargs):
        return "REDIS"
        # try:
        #     return "REDIS:" + args[1]
        # except:
        #     return "REDIS:" + func.__name__

    def redis_processor(func, span, *args, **kwargs):
        span.set_tag(logs.MESSAGE, args[1] + " " + args[2])
    from redis.client import Redis
    Redis.execute_command = tracing_enabled(Redis.execute_command, name_generator=redis_name, span_processor=redis_processor)
