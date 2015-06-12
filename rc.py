#!/usr/bin/env python3

import os
import csv
import shutil
from multiprocessing.pool import ThreadPool
from redcap import Project
from constants import MODEL_COLUMNS, TABLE_COLUMNS, FIELD_COLUMNS, \
    SCHEMA_COLUMNS


# Use same field names as returned by the API.
csv_header = (
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
    for t in s.split(' | '):
        c = t.split(',', 1)

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

    pool = ThreadPool()

    for table, data in tables.items():
        args = (root, model, version, table, data)
        pool.apply_async(generate_table_files, args=args)

    pool.close()
    pool.join()


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


def _patch_redcap():
    """
    Patches _call_api to bypass https://github.com/sburns/PyCap/issues/54
    since it is not relevant in this context.
    """
    __call_api = Project._call_api

    def _call_api(self, payload, typpe, **kwargs):
        if typpe == 'exp_event':
            return [{'error': 'monkey patched'}]

        return __call_api(self, payload, typpe, **kwargs)

    Project._call_api = _call_api


def main(argv=None):
    usage = """REDCap Data Model Generator

    Usage: redcap <model> <version> (<path> | <url> <token>) [--dir=DIR]

    Options:
        -h --help       Show this screen.
        --dir=DIR       Name of the directory to output the files.

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
    if args['<path>']:
        with open(args['<path>'], 'rU') as f:
            # Skip header
            next(f)
            r = csv.DictReader(f, fieldnames=csv_header)
            fields = list(r)
    else:
        _patch_redcap()

        project = Project(args['<url>'], args['<token>'])
        fields = project.export_metadata(format='json')

    generate(fields, args['<model>'], args['<version>'], args['--dir'])


if __name__ == '__main__':
    main()
