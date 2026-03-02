"""Simple helper for creating the PostgreSQL table used by the scraper.

Usage example:

    # either set the variables in your shell or create a ".env" file at the
    # repository root containing those values; the script will automatically
    # load them via python-dotenv when it starts.
    # example .env contents:
    #     PGHOST=localhost
    #     PGPORT=5432
    #     PGUSER=myuser
    #     PGPASSWORD=mypassword
    #     PGDATABASE=houses

    python -m src.utils.db_setup

The script will connect to the database and create a table called
``properties`` with the columns referenced by the scraper.
"""
import os
import sys

# load environment variables from .env if present
from dotenv import load_dotenv

import psycopg2
from psycopg2 import sql

# attempt reading .env in project root; this is a no-op if the file doesn't exist
load_dotenv()


def get_connection(dbname=None):
    """Return a new psycopg2 connection using environment variables.
    The standard postgresql client environment variables are respected:
    ``PGHOST``, ``PGPORT``, ``PGDATABASE``, ``PGUSER`` and ``PGPASSWORD``.

    If ``dbname`` is provided it takes precedence; otherwise the value of
    ``PGDATABASE`` is used (which may be ``None``).
    """
    try:
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", 5432),
            dbname=dbname or os.environ.get("PGDATABASE"),
            user=os.environ.get("PGUSER"),
            password=os.environ.get("PGPASSWORD"),
        )
    except Exception as e:
        print("failed to connect to the database:\n", e)
        sys.exit(1)
    return conn


def create_properties_table(conn):
    """Create the ``properties`` table with the expected schema.

    Columns mirror the keys printed by ``scraper.py``.
    """
    create_sql = sql.SQL(
        """
        create table if not exists properties (
            id              text primary key,
            ref_id          text,
            is_sold         text,
            property_type   text,
            property_subtype text,
            is_new_build    text,
            building_year   text,
            mandate         text,
            description     text,
            price           text,
            price_min       text,
            price_max       text,
            property_surface text,
            min_property_surface text,
            max_property_surface text,
            floor_number    text,
            rooms_count     text,
            min_rooms_count text,
            max_rooms_count text,
            energy          text,
            geo             text,
            country         text,
            region          text,
            city_name       text
        );
        """
    )
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    print("table 'properties' is ready")


def create_database(conn, name="housesagent"):
    """Create a database with the given name if it doesn't already exist."""
    # postgres does not support "create database if not exists", so we check
    with conn.cursor() as cur:
        cur.execute(
            "select 1 from pg_database where datname = %s", (name,)
        )
        exists = cur.fetchone() is not None
        if not exists:
            conn.autocommit = True
            cur.execute(sql.SQL("create database {};").format(sql.Identifier(name)))
            conn.autocommit = False
            print(f"database '{name}' created")
        else:
            print(f"database '{name}' already exists")


if __name__ == "__main__":
    # initial connection does not specify a database (or uses PGDATABASE if
    # already set); it's used solely to ensure the target database exists.
    db_name = os.environ.get("PGDATABASE", "postgres")
    init_conn = get_connection(db_name)
    create_database(init_conn, db_name)
    init_conn.close()

    # now reconnect to the actual housesagent database to create the table
    target_conn = get_connection(db_name)
    create_properties_table(target_conn)
    target_conn.close()
