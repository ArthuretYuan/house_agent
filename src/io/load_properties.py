"""Fetch listings with the scraper and persist them into Postgres.

Usage:

    export PGHOST=...
    export PGUSER=...
    export PGDATABASE=housesagent   # or whatever DB you created
    export SCRAPE_URL="https://www.athome.lu/vente?page=5"  # optional

    python -m src.io.load_properties

The script scrapes the URL (falls back to a hardcoded default), converts the
returned dictionaries' keys to match the ``properties`` table columns, and
inserts/upserts each row.
"""

import os
from typing import List
import sys
from psycopg2 import sql
import json

root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root not in sys.path:
    sys.path.insert(0, root) 

from src.utils.scraper import property_scraper
from src.io.db_setup import get_connection


# mapping from keys produced by the scraper to column names in the table
# note: the table has its own primary key `id` and the scraper's `id` value
# should be stored in the `ref_id` column instead.
KEY_TO_COLUMN = {
    "id": "ref_id",
    "isSoldProperty": "is_sold",
    "propertyType": "property_type",
    "propertySubType": "property_subtype",
    "isNewBuild": "is_new_build",
    "buildingYear": "building_year",
    "mandate": "mandate",
    "description": "description",
    "price": "price",
    "price_min": "price_min",
    "price_max": "price_max",
    "propertySurface": "property_surface",
    "minPropertySurface": "min_property_surface",
    "maxPropertySurface": "max_property_surface",
    "floorNumber": "floor_number",
    "roomsCount": "rooms_count",
    "minRoomsCount": "min_rooms_count",
    "maxRoomsCount": "max_rooms_count",
    "energy": "energy",
    "geo": "geo",
}


import uuid

def convert_record(scraped: dict) -> dict:
    """Return a new dictionary whose keys match table column names.

    The table defines its own primary key ``id`` which we generate as a
    UUID; the value scraped under ``id`` is stored in ``ref_id`` per
    :data:`KEY_TO_COLUMN`.  All other values remain strings.

    The ``geo`` column normally holds a JSON-like string; we parse that and
    extract ``country``, ``region`` and ``cityName`` into separate keys that
    correspond to the additional columns you mentioned.  If parsing fails we
    just leave those values empty.
    """
    base = {col: scraped.get(key, "") for key, col in KEY_TO_COLUMN.items()}
    base.setdefault("id", uuid.uuid4().hex)

    # unpack geo field if possible
    geo_str = scraped.get("geo", "").replace("'", '"')  # handle single quotes if present; may still fail if malformed
    geo_str = geo_str.replace('d"or', "d'or")
    country = region = city_name = ""
    try:
        geo_obj = json.loads(geo_str)
        country = geo_obj.get("country", "")
        region = geo_obj.get("region", "")
        # scraped key is cityName but our column might be city_name
        city_name = geo_obj.get("cityName", "")
    except Exception:
        # geo_str may already be a Python dict or malformed; guard silently
        if isinstance(geo_str, dict):
            country = geo_str.get("country", "")
            region = geo_str.get("region", "")
            city_name = geo_str.get("cityName", "")
        print(f"Failed to parse geo field: {geo_str}")
    base["country"] = country
    base["region"] = region
    base["city_name"] = city_name
    return base


def insert_properties(conn, records: List[dict]):
    """Insert or update the given list of records into ``properties``."""
    if not records:
        return

    columns = list(records[0].keys())
    # build a single upsert statement
    insert = sql.SQL("insert into properties ({cols}) values ({placeholders})")
    insert = insert.format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    # set clause for conflict target id
    update_columns = [c for c in columns if c != "id"]
    if update_columns:
        update_clause = sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
            for c in update_columns
        )
        upsert = sql.SQL("{insert} on conflict (id) do update set {update}").format(
            insert=insert, update=update_clause
        )
    else:
        upsert = sql.SQL("{insert} on conflict (id) do nothing").format(insert=insert)

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(upsert, [rec[c] for c in columns])
    conn.commit()


if __name__ == "__main__":
    urls = ["https://www.athome.lu/vente?page=1",
            "https://www.athome.lu/vente?page=2",
            "https://www.athome.lu/vente?page=3",
            "https://www.athome.lu/vente?page=4",
            "https://www.athome.lu/vente?page=5",
            "https://www.athome.lu/vente?page=6"]
    for url in urls:
        print(f"Processing {url}...")
        scraped = property_scraper(url, is_save_json=False)
        normalized = [convert_record(r) for r in scraped]
        if normalized:
            conn = get_connection()
            insert_properties(conn, normalized)
            conn.close()
            print(f"inserted/updated {len(normalized)} rows into properties")
        else:
            print("no records to insert")
