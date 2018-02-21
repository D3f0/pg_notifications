#!/usr/bin/env python

import importlib
import select
from contextlib import contextmanager
from functools import partial
from multiprocessing import Process, Queue

import click
import psycopg2
import psycopg2.extensions
from jinja2 import Template
from psycopg2.extras import RealDictCursor


TRIGGER_FUNCTION = """
    CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$

        DECLARE
            data json;
            notification json;

        BEGIN

            -- Convert the old or new row to JSON, based on the kind of action.
            -- Action = DELETE?             -> OLD row
            -- Action = INSERT or UPDATE?   -> NEW row
            IF (TG_OP = 'DELETE') THEN
                data = row_to_json(OLD);
            ELSE
                data = row_to_json(NEW);
            END IF;

            -- Contruct the notification as a JSON string.
            notification = json_build_object(
                            'table',TG_TABLE_NAME,
                            'action', TG_OP,
                            'data', data);


            -- Execute pg_notify(channel, notification)
            PERFORM pg_notify('events',notification::text);

            -- Result is ignored since this is an AFTER trigger
            RETURN NULL;
        END;

    $$ LANGUAGE plpgsql;
"""

ADD_TRIGGER_TO_TABLE = Template("""\
DROP TRIGGER  IF EXISTS {{table}}_notify_event ON {{table}};
CREATE TRIGGER {{table}}_notify_event
AFTER INSERT OR UPDATE OR DELETE ON {{table}}
    FOR EACH ROW EXECUTE PROCEDURE notify_event();
    COMMIT;
""")


GET_TABLE_TRIGGERS = Template("""\
SELECT event_object_table
      ,trigger_name
      ,event_manipulation
      ,action_statement
      ,action_timing
FROM  information_schema.triggers
WHERE event_object_table = '{{ table }}'
ORDER BY event_object_table
     ,event_manipulation
""")


def get_associated_triggers(conn, table):
    """Given a table name, return a list of triggers"""
    sql = GET_TABLE_TRIGGERS.render(table=table)

def get_tables(ctx):
    """Get tables of current schema"""
    with ctx.obj.get_cursor() as cursor:
        cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public'
        """.format(**locals()))
        data = [row[0] for row in cursor.fetchall()]
        return data

@contextmanager
def cursor_wrapper(conn, cursor_factory=psycopg2.extensions.cursor):
    """
    Context manager to generate a cursor
    """
    cursor = conn.cursor(cursor_factory=cursor_factory)
    yield cursor
    cursor.close()

@click.group()
@click.option('--verbose',
              is_flag=True,
              help='Show SQL')
@click.option('--conn',
              #prompt='Connection',
              default="dbname=nguru",
              help='The connection string')
@click.pass_context
def cli(ctx, conn, verbose):
    try:
        ctx.obj['CONN'] = psycopg2.connect(conn)
    except psycopg2.OperationalError:
        raise click.BadArgumentUsage("Error connecting to postgres with conn={}".format(conn))
    ctx.obj['DBNAME'] = 'nguru'
    ctx.obj['VERBOSE'] = verbose

    ctx.obj['get_cursor'] = partial(cursor_wrapper, ctx.obj.CONN)

@cli.command()
@click.argument('tables', nargs=-1)
@click.pass_context
def install(ctx, tables):
    not_found = set(tables).difference(set(get_tables(ctx)))
    if not_found:
        tables = ','.join(list(not_found))
        raise click.BadArgumentUsage("Tables {} could not be found".format(tables))
    for table in tables:
        sql = ADD_TRIGGER_TO_TABLE.render(table=table)
        cursor = ctx.obj['CONN'].cursor()
        if ctx.obj['VERBOSE']:
            click.echo(sql)
        fail = cursor.execute(sql)

        if fail:
            print(fail)
        else:
            click.echo("OK: {}".format(table))

def get_table_triggers(ctx, tables):
    with ctx.obj.get_cursor() as cursor:
        retval = {}
        for table in tables:
            sql = GET_TABLE_TRIGGERS.render(table=table)
            cursor.execute(sql)
            click.echo(sql)
            if cursor.rowcount < 1:
                continue
            for _, trigger_name, event, action, when in cursor.fetchall():
                events = retval.setdefault(event, [])
                events.append((action, when))
        return retval

@cli.command()
@click.pass_context
def list_tables(ctx):

    tables = get_tables(ctx)
    tirggers = get_table_triggers(ctx, tables)
    import ipdb ; ipdb.set_trace()
    for table in get_tables(ctx):
        click.echo("* {}".format(table))

@cli.command()
@click.pass_context
def list_triggers(ctx):
    tables = get_tables(ctx)
    tirggers = get_table_triggers(ctx, tables)
    for table in get_tables(ctx):
        click.echo("* {}".format(table))

def iter_events(ctx, channel='events', timeout=None):
    conn = ctx.obj['CONN']
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    curs = conn.cursor()
    curs.execute("LISTEN {};".format(channel))

    print ("Waiting for notifications on channel '{}'".format(channel))
    while True:
        if select.select([conn],[],[],timeout or None) == ([],[],[]):
            print("Timeout")
        else:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                yield (notify.pid, notify.channel, notify.payload)


@cli.command()
@click.pass_context
def get_triggers(ctx, ):
    with ctx.obj.get_cursor() as cursor:
        cursor.execute("select * from mara_di;")
        click.echo(cursor.fetchall())

@cli.command()
@click.option('--timeout', type=int, default=0)
@click.option('--callback', type=str, default=None, help="Python function to call")
@click.option('--ipc', is_flag=True, help="Run callback in another process")
@click.pass_context
def watch(ctx, timeout, callback, ipc):
    function = None
    if callback:
        mod_name, func_name = callback.rsplit('.', 1)
        mod = importlib.import_module(mod_name)
        function = getattr(mod, func_name, None)
        if not callable(function):
            raise click.BadArgumentUsage("{} could not be imported".format(callback))

    if ipc:
        if not function:
            raise click.BadArgumentUsage("IPC specified but no callback")

        def listen_to_queue(queue, ctx):
            for event in iter_events(ctx):
                queue.put(event)
        def queue_to_callback(queue):
            while True:
                element = queue.get()
                function(element)
        queue = Queue()
        back = Process(target=listen_to_queue, args=(queue, ctx))
        front = Process(target=queue_to_callback, args=(queue, ))
        back.join()
        front.join()

    elif function:
        for event in iter_events(ctx):
            function(event)
    else:
        for event in iter_events(ctx):
            click.echo("Received event: {}".format(event))


class AttrDict(dict):
    """Use dot notation instead of inexing"""
    __getattr__ = dict.__getitem__


if __name__ == '__main__':
    cli(obj=AttrDict())
