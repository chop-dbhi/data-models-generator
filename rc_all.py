#!/usr/bin/env python3

import os
import shutil
from getpass import getpass
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy.sql import text
from rc import db_connect, db_metadata, generate


def worker(project, index, args):
    conn = db_connect(args['--db'],
                      args['--host'],
                      args['--port'],
                      args['--user'],
                      args['--pass'])

    fields = db_metadata(conn, project)

    if not fields:
        return

    rootdir = os.path.join(args['<dir>'],
                           project,
                           args['<version>'])

    if os.path.exists(rootdir):
        shutil.rmtree(rootdir)

    os.makedirs(rootdir)

    generate(fields, project, args['<version>'], rootdir)


def db_projects(conn):
    sql = text('''
        SELECT
            project_name
        FROM
            redcap_projects
    ''')

    query = conn.execute(sql)

    return [row[0] for row in query]


def main(argv=None):
    usage = """REDCap Data Model Generator

    Usage:
        redcap dball <version> [--dir=DIR] [--db=DB] [--host=HOST] [--port=PORT] [--user=USER] [--pass=PASS]

    Options:
        -h --help       Show this screen.
        --dir=DIR       Name of the directory to output the files [default: .].
        --db=DB         Name of the REDCap database [default: redcap].
        --host=HOST     Host of the database server [default: localhost].
        --port=PORT     Port of the database server [default: 3306].
        --user=USER     Username to connect with.
        --pass=PASS     Password to connect with. If set to *, a prompt will be provided.
        --procs=PROCS   Number of processes to spawn [default: 24].

    """  # noqa

    from docopt import docopt

    args = docopt(usage, argv=argv, version='0.1')

    if args['--pass'] == '*':
        args['--pass'] = getpass('password: ')

    conn = db_connect(args['--db'],
                      args['--host'],
                      args['--port'],
                      args['--user'],
                      args['--pass'])

    project_names = db_projects(conn)

    pool = ProcessPoolExecutor(max_workers=int(args['--procs']))

    for name in project_names:
        pool.submit(worker, name, args)

    pool.shutdown()


if __name__ == '__main__':
    main()
