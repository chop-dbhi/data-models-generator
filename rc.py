#!/usr/bin/env python3

import os
import csv
import shutil
from getpass import getpass
from concurrent.futures import ThreadPoolExecutor
from redcap import Project
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine.url import URL
from constants import MODEL_COLUMNS, TABLE_COLUMNS, FIELD_COLUMNS, \
    SCHEMA_COLUMNS
import _redcap

_redcap.patch()


# Standard set of fields for REDCap metadata.
redcap_fields = (
    'field_name',
    'form_name',
    'section_header',
    'field_type',
    'field_label',
    'select_choices_or_calculations',
    'field_note',
    'text_validation_type_or_show_slider_number',
    'text_validation_min',
    'text_validation_max',
    'identifier',
    'branching_logic',
    'required_field',
    'custom_alignment',
    'question_number',
    'matrix_group_name',
    'matrix_ranking',
)


def db_connect(database, host, port, user, password):
    # Construct engine URL
    url = URL('mysql+pymysql',
              username=user,
              password=password,
              host=host,
              port=port,
              database=database)

    return create_engine(url)


def db_metadata(conn, project):
    "Returns records from a REDCap database."

    sql = text('''
        SELECT
            field_name,
            form_name,
            element_preceding_header as section_header,
            element_type as field_type,
            element_label as field_label,
            element_enum as choices_calc_slider,
            element_note as field_note,
            element_validation_type as validation_type,
            element_validation_min as validation_min,
            element_validation_max as validation_max,
            field_phi as identifier,
            branching_logic,
            field_req as required,
            custom_alignment,
            question_num as question_number,
            grid_name as matrix_group_name
        FROM redcap_metadata JOIN redcap_projects
            ON (redcap_metadata.project_id = redcap_projects.project_id)
        WHERE redcap_projects.project_name = :project
        ORDER BY field_order
    ''')

    query = conn.execute(sql, project=project)

    return [dict(zip(redcap_fields, row)) for row in query]


def get_field_type(field):
    "Returns the data models field type based on properties of the field."
    val_type = field['text_validation_type_or_show_slider_number']

    if val_type == 'date_ymd':
        return 'date'

    return 'string'


def parse_choices(s):
    "Parses and returns field choices."
    choices = []

    if not s:
        return

    # Split by pipe, then by the first comma.
    for t in s.split('|'):
        c = t.split(',', 1)

        # Could not parse, assume it is a calculation intead.
        if len(c) < 2:
            return

        choices.append(c[1].strip())

    return choices


def get_field_description(field):
    "Combines section header, field note and choices to form a description."
    toks = []

    if field['field_note']:
        toks.append(field['field_note'])

    if field['section_header']:
        toks.append('Under section {}.'.format(field['section_header']))

    choices = parse_choices(field['select_choices_or_calculations'])

    if choices:
        toks.append('Choices include: {}'.format(', '.join(choices)))

    return ' '.join(toks)


def generate(rc_fields, model, version, root):
    # Models file.
    with open(os.path.join(root, 'models.csv'), 'w') as f:
        w = csv.writer(f)
        w.writerows([
            MODEL_COLUMNS,
            (model, version, '', '', ''),
        ])

    tables = {}

    for f in rc_fields:
        # Treat the form as the table. The section will be included
        # in the field description if one is present.
        table = f['form_name']

        field = f['field_name']

        # Initialize table entry. Constraints could be included
        # for the `identifier` and `field_required` fields.
        if table not in tables:
            tables[table] = {
                'fields': [],
                'schemata': [],
            }

        # Add field to table.
        tables[table]['fields'].append((
            model,
            version,
            table,
            field,
            f['field_label'],
            get_field_description(f),
        ))

        # Add field schema to table.
        tables[table]['schemata'].append((
            model,
            version,
            table,
            field,
            get_field_type(f),
            '',  # length
            '',  # precision
            '',  # scale
            '',  # default
        ))

    # Tables file.
    generate_tables(root, model, version, tables.keys())

    workers = os.cpu_count()
    pool = ThreadPoolExecutor(max_workers=workers)

    for table, data in tables.items():
        pool.submit(generate_table_files, root, model, version, table, data)

    pool.shutdown()


def generate_tables(dirname, model, version, tables):
    "Creates a tables file."
    fn = os.path.join(dirname, 'tables.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(TABLE_COLUMNS)

        for table in tables:
            w.writerow([
                model,
                version,
                table,
                '',
            ])


def generate_table_files(dirname, model, version, table, data):
    "Creates a field and schema files."
    dirname = os.path.join(dirname, table)

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    fn = os.path.join(dirname, 'fields.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(FIELD_COLUMNS)
        w.writerows(data['fields'])

    fn = os.path.join(dirname, 'schema.csv')

    with open(fn, 'w') as f:
        w = csv.writer(f)
        w.writerow(SCHEMA_COLUMNS)
        w.writerows(data['schemata'])


def main(argv=None):
    usage = """REDCap Data Model Generator

    Usage:
        redcap csv  <model> <version> <path>        [--dir=DIR]
        redcap api  <model> <version> <url> <token> [--dir=DIR]
        redcap db   <model> <version> <project>     [--dir=DIR] [--db=DB] [--host=HOST] [--port=PORT] [--user=USER] [--pass=PASS]

    Options:
        -h --help       Show this screen.
        --dir=DIR       Name of the directory to output the files.
        --db=DB         Name of the REDCap database [default: redcap].
        --host=HOST     Host of the database server [default: localhost].
        --port=PORT     Port of the database server [default: 3306].
        --user=USER     Username to connect with.
        --pass=PASS     Password to connect with. If set to *, a prompt will be provided.

    """  # noqa

    from docopt import docopt

    args = docopt(usage, argv=argv, version='0.1')

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

    # File path
    if args['csv']:
        with open(args['<path>'], 'rU') as f:
            # Skip header
            next(f)
            r = csv.DictReader(f, fieldnames=redcap_fields)
            fields = list(r)

    elif args['api']:
        project = Project(args['<url>'], args['<token>'])
        fields = project.export_metadata(format='json')

    elif args['db']:
        if args['--pass'] == '*':
            args['--pass'] = getpass('password: ')

        conn = db_connect(args['--db'],
                          args['--host'],
                          args['--port'],
                          args['--user'],
                          args['--pass'])

        fields = db_metadata(conn, args['<project>'])

    generate(fields, args['<model>'], args['<version>'], args['--dir'])


if __name__ == '__main__':
    main()
