import io

import yaml
from psycopg2 import sql
from sqlalchemy import create_engine


def ps_connect(database_name):
    # Connects to Postgres database using credentials specified in config.yaml
    #
    # Args:
    #   database_name: name of database in config.ymal
    #
    # Returns:
    #   SQLAlchemy engine for specified database
    with open('config.yaml', 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)

    user = cfg[database_name]['user']
    password = cfg[database_name]['password']
    database_url = cfg[database_name]['database_url']

    connection_string = f"postgresql://{user}:{password}@{database_url}"
    engine = create_engine(connection_string) 
    return engine