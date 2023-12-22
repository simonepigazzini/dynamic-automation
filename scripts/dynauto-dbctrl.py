#!/usr/bin/env python3

import argparse
import logging
import json
import sys
from influxdb import InfluxDBClient
from pathlib import Path
from dynauto.credentials import dbhost, dbport, dbusr, dbpwd, dbssl


def cmdline_options():
    """
    Return the argparse instance to handle the cmdline options for this script.

    The function is needed by sphinx-argparse to easily generate the docs.
    """
    parser = argparse.ArgumentParser(description="""
    Create a new database including users.
    """, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--db',
                        dest='db',
                        required=True,
                        type=str,
                        help='Database name')
    parser.add_argument('--users-config',
                        dest='users_config',
                        required=True,
                        type=Path,
                        help='Users configuration json file path')

    return parser


if __name__ == '__main__':
    opts = cmdline_options().parse_args()

    dbclient = InfluxDBClient(host=dbhost,
                              port=dbport,
                              username=dbusr,
                              password=dbpwd,
                              ssl=dbssl,
                              database=opts.db,
                              timeout=30_000)

    dbclient.create_database(opts.db)
    with open(opts.users_config, 'r') as fusers:
        users = json.load(fusers)
        for user, config in users.items():
            dbclient.create_user(user, config['password'])
            dbclient.grant_privilege(config['rights'], opts.db, user)
            logging.info(f"Added user {user} to db {opts.db}")

    logging.info(f"db {opts.db} created.")

    sys.exit(0)
