#!/usr/bin/env python3

import sys
import sql
import rc


def main():
    usage = """Data Models Generator

    Usage: main.py (sql | redcap) <args>...

    Options:
        -h --help       Show this screen.

        --dir=DIR       Name of the directory to output the files.

    """  # noqa

    from docopt import docopt

    # Ignore command name.
    argv = sys.argv[1:]

    args = docopt(usage, argv=argv, version='0.1')

    # Trim subcommand.
    sub_argv = argv[1:]

    if args['sql']:
        sql.main(sub_argv)
    elif args['redcap']:
        rc.main(sub_argv)


if __name__ == '__main__':
    main()
