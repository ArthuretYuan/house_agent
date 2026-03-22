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


# the scraper now returns keys that correspond directly to the table
# columns (the primary key is the scraped ``id`` itself), so the
# conversion step is trivial.  We keep the mapping object in case we want
# to adjust field names in the future, but for now it is basically
# identity.
KEY_TO_COLUMN = {  # values are column names; keys are scraped field names
    "id": "id",
    "type": "type",
    "permalink": "permalink",
    "isNewBuild": "isNewBuild",
    "createdAt": "createdAt",
    "updatedAt": "updatedAt",
    "price": "price",
    "soldPrice": "soldPrice",
    "baselinePrice": "baselinePrice",
    "previewDescriptions": "previewDescriptions",
    "street": "street",
    "postalCode": "postalCode",
    "city": "city",
    "country": "country",
    "rooms": "rooms",
    "bedrooms": "bedrooms",
    "bathrooms": "bathrooms",
    "showers": "showers",
    "basement": "basement",
    "garages": "garages",
    "indoorParking": "indoorParking",
    "outdoorParking": "outdoorParking",
    "surface": "surface",
    "groundSurface": "groundSurface",
}



def convert_record(scraped: dict) -> dict:
    """Normalize a scraped dictionary to match the ``properties`` table.

    The new scraper already emits keys that correspond one-to-one with the
    columns defined in :func:`create_properties_table` (see
    :mod:`src.io.db_setup`).  We simply copy values over and provide empty
    strings for any missing fields so that the insertion logic downstream can
    rely on a consistent set of keys.
    """
    # ensure every column exists in the returned dict; missing keys become
    # empty strings
    rec = {col: scraped.get(key, "") for key, col in KEY_TO_COLUMN.items()}
    return rec


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
    urls = [f"https://www.athome.lu/vente?page={i}" for i in range(1, 100)]
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
