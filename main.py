#!/usr/bin/env python3

import os
import csv
import shutil
from getpass import getpass
from multiprocessing.pool import ThreadPool
from sqlalchemy import inspect, create_engine, types
from sqlalchemy.engine.url import URL

model_columns = (
    'model',
    'version',
    'label',
    'url',
    'description',
)

table_columns = (
    'model',
    'version',
    'table',
    'description',
)

field_columns = (
    'model',
    'version',
    'table',
    'field',
    'description',
)

schema_columns = (
    'model',
    'version',
    'table',
    'field',
    'type',
    'length',
    'precision',
    'scale',
    'default',
)

constraint_columns = (
    'model',
    'version',
    'table',
    'field',
    'type',
    'name',
)

index_columns = (
    'model',
    'version',
    'table',
    'field',
    'name',
    'order',
)


reference_columns = (
    'model',
    'version',
    'table',
    'field',
    'ref_table',
    'ref_field',
    'name',
)


def map_field_attrs(col):
    typ = col['type']

    # Map simple type.
    if isinstance(typ, types.Integer):
        col['dm_type'] = 'integer'
    elif isinstance(typ, types.Boolean):
        col['dm_type'] = 'boolean'
    elif isinstance(typ, types.Date):
        col['dm_type'] = 'date'
    elif isinstance(typ, types.DateTime):
        col['dm_type'] = 'datetime'
    elif isinstance(typ, types.Float):
        col['dm_type'] = 'number'
    elif isinstance(typ, types._Binary):
        col['dm_type'] = 'bytes'
    elif isinstance(typ, types.Numeric):
        col['dm_type'] = 'number'
    elif isinstance(typ, types.String):
        col['dm_type'] = 'string'

    # Ignore Postgres sequence-based defaults.
    if col['default'] and col['default'].startswith('nextval'):
        col['default'] = ''


def create(engine, model, version, root):
    with open(os.path.join(root, 'models.csv'), 'w') as f:
        w = csv.writer(f)
        w.writerows([
            model_columns,
            (model, version, '', '', ''),
        ])

    inspector = inspect(engine)

    tables = inspector.get_table_names()
    generate_tables(root, model, version, tables)

    pool = ThreadPool()

    for table in tables:
        args = (inspector, root, model, version, table)
        pool.apply_async(generate_table_files, args=args)

    pool.close()
    pool.join()


def generate_table_files(inspector, dirname, model, version, table):
    dirname = os.path.join(dirname, table)

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    generate_fields(inspector, dirname, model, version, table)
    generate_references(inspector, dirname, model, version, table)
    generate_indexes(inspector, dirname, model, version, table)
    generate_constraints(inspector, dirname, model, version, table)


def generate_tables(dirname, model, version, tables):
    "Creates a tables file."
    fn = os.path.join(dirname, 'tables.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(table_columns)

        for table in tables:
            w.writerow([
                model,
                version,
                table,
                '',
            ])


def generate_fields(inspector, dirname, model, version, table):
    "Creates a fields file."
    fields = inspector.get_columns(table)

    if not fields:
        return

    fn = os.path.join(dirname, 'fields.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(field_columns)

        for field in fields:
            w.writerow([
                model,
                version,
                table,
                field['name'],
                '',
            ])

    fn = os.path.join(dirname, 'schema.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(schema_columns)

        for field in fields:
            map_field_attrs(field)

            w.writerow([
                model,
                version,
                table,
                field['name'],
                field['dm_type'],
                getattr(field['type'], 'length', ''),
                getattr(field['type'], 'precision', ''),
                getattr(field['type'], 'scale', ''),
                field['default'],
            ])


def generate_references(inspector, dirname, model, version, table):
    refs = inspector.get_foreign_keys(table)

    if not refs:
        return

    fn = os.path.join(dirname, 'references.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(reference_columns)

        for ref in refs:
            cols = ref['constrained_columns']
            rcols = ref['referred_columns']

            for i, col in enumerate(cols):
                w.writerow([
                    model,
                    version,
                    table,
                    col,
                    ref['referred_table'],
                    rcols[i],
                    ref['name'],
                ])


def generate_indexes(inspector, dirname, model, version, table):
    indexes = inspector.get_indexes(table)

    if not indexes:
        return

    fn = os.path.join(dirname, 'indexes.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(index_columns)

        for idx in indexes:
            for col in idx['column_names']:
                w.writerow([
                    model,
                    version,
                    table,
                    col,
                    idx['name'],
                    '',
                ])


def generate_constraints(inspector, dirname, model, version, table):
    fn = os.path.join(dirname, 'constraints.csv')

    fields = inspector.get_columns(table)

    if not fields:
        return

    not_nulls = [f for f in fields if not f['nullable']]

    pk = inspector.get_pk_constraint(table)
    uniques = inspector.get_unique_constraints(table)

    if not pk and not uniques and not not_nulls:
        return

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(constraint_columns)

        for col in pk['constrained_columns']:
            w.writerow([
                model,
                version,
                table,
                col,
                'primary key',
                pk['name'],
            ])

        for uniq in uniques:
            for col in uniq['column_names']:
                w.writerow([
                    model,
                    version,
                    table,
                    col,
                    'unique',
                    uniq['name'],
                ])

        for field in not_nulls:
            w.writerow([
                model,
                version,
                table,
                field['name'],
                'not null',
                '',
            ])


if __name__ == '__main__':
    usage = """Data Models Generator

    Usage: data-models-generator.py <model> <version> <engine> <database> [--dir=DIR] [--host=HOST] [--port=PORT] [--user=USER] [--pass=PASS]

    Options:
        -h --help       Show this screen.

        --dir=DIR           Name of the directory to output the files. Defaults to the database name.
        --host=HOST         Host of the database server. Defaults to localhost.
        --port=PORT         Port of the database server. Defaults to default port for the engine.
        --user=USER         Username to connect with.
        --pass=PASS         Password to connect with. If set to *, a prompt will be provided.

    """  # noqa

    from docopt import docopt

    args = docopt(usage, version='0.1')

    if args['--pass'] == '*':
        args['--pass'] = getpass('password: ')

    # Default to a directory named after the database.
    if not args['--dir']:
        args['--dir'] = os.path.join(os.getcwd(),
                                     args['<model>'],
                                     args['<version>'])

    # Ensure the output directory is created.
    rootdir = args['--dir']

    if os.path.exists(rootdir):
        shutil.rmtree(rootdir)

    os.makedirs(rootdir)

    url = URL(args['<engine>'],
              username=args['--user'],
              password=args['--pass'],
              host=args['--host'],
              port=args['--port'],
              database=args['<database>'])

    engine = create_engine(url)

    create(engine, args['<model>'], args['<version>'], args['--dir'])
