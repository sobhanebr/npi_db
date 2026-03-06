#!/usr/bin/env python3
"""
NPI Data ETL – First Normal Form (1NF) Loader
=============================================
Reads all four NPPES dissemination files and populates the 1NF schema
defined in schema_1nf.sql.

Files consumed
--------------
  npidata_pfile_20050523-20260208.csv       → provider, taxonomy,
                                              provider_taxonomy,
                                              other_provider_identifier
  othername_pfile_20050523-20260208.csv     → other_name
  pl_pfile_20050523-20260208.csv            → secondary_practice_location
  endpoint_pfile_20050523-20260208.csv      → endpoint

Usage
-----
  # Activate env: source .conda/bin/activate  (or conda activate .conda)
  # Set env vars, or leave NPI_DB_PASSWORD unset to be prompted:
  NPI_DB_HOST=localhost NPI_DB_USER=root NPI_DB_NAME=npi_db python3 etl_load_1nf.py

Dependencies
------------
  pip install PyMySQL  (install in .conda venv to avoid externally-managed-environment)
"""

import csv
import getpass
import os
import sys
import time
from datetime import datetime, date
from typing import Optional

import pymysql
import pymysql.cursors

# ── Configuration ─────────────────────────────────────────────────────────────

DB_CONFIG: dict = {
    "host":     os.getenv("NPI_DB_HOST",     "localhost"),
    "port":     int(os.getenv("NPI_DB_PORT", "3306")),
    "user":     os.getenv("NPI_DB_USER",     "root"),
    "password": os.getenv("NPI_DB_PASSWORD", ""),
    "database": os.getenv("NPI_DB_NAME",     "npi_db"),
    "charset":  "utf8mb4",
    "autocommit": False,
}

DATA_DIR:   str = os.getenv("NPI_DATA_DIR", "./data")
BATCH_SIZE: int = int(os.getenv("NPI_BATCH_SIZE", "2000"))

MAX_PASSWORD_RETRIES = 3
MYSQL_ERR_ACCESS_DENIED = 1045


def get_db_password(force_prompt: bool = False) -> str:
    """Get password from env or prompt. Use force_prompt=True on retries to avoid reusing wrong env value."""
    if force_prompt or not os.getenv("NPI_DB_PASSWORD"):
        return getpass.getpass("Database password: ")
    return os.getenv("NPI_DB_PASSWORD", "")


def connect_with_retry() -> pymysql.Connection:
    """Connect to DB, prompting for password if needed; retry up to 3 times on auth failure."""
    config = dict(DB_CONFIG)
    for attempt in range(1, MAX_PASSWORD_RETRIES + 1):
        config["password"] = get_db_password(force_prompt=(attempt > 1))
        try:
            return pymysql.connect(**config)
        except pymysql.err.OperationalError as exc:
            errno = getattr(exc, "args", (None,))[0] if exc.args else None
            if errno == MYSQL_ERR_ACCESS_DENIED and attempt < MAX_PASSWORD_RETRIES:
                print(f"Access denied (attempt {attempt}/{MAX_PASSWORD_RETRIES}). Try again.", file=sys.stderr)
                continue
            raise
    raise RuntimeError("Unreachable")


# ── Helpers ────────────────────────────────────────────────────────────────────

def v(row: list, idx: int) -> Optional[str]:
    """Return stripped cell value, or None for empty / out-of-range cells."""
    if idx < 0:
        return None
    try:
        s = row[idx].strip()
        return s if s else None
    except IndexError:
        return None


def parse_date(val: str) -> Optional[date]:
    """Parse MM/DD/YYYY → datetime.date, returning None on failure."""
    s = val.strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        return None


def progress(label: str, count: int, start: float) -> None:
    elapsed = time.time() - start
    rate = count / elapsed if elapsed else 0
    print(f"\r  {label}: {count:>10,}  |  {rate:>8,.0f} rows/s  |  {elapsed:>6.1f}s elapsed",
          end="", flush=True)


# ── Phase 1: Main NPI file ─────────────────────────────────────────────────────

PROVIDER_SQL = """
INSERT IGNORE INTO provider (
    npi, entity_type_code, replacement_npi,
    org_name, ein,
    last_name, first_name, middle_name, name_prefix, name_suffix, credential, sex_code,
    other_org_name, other_org_name_type_code,
    other_last_name, other_first_name, other_middle_name,
    other_name_prefix, other_name_suffix, other_credential, other_last_name_type_code,
    mailing_address_line1, mailing_address_line2, mailing_city, mailing_state,
    mailing_postal_code, mailing_country_code, mailing_telephone, mailing_fax,
    practice_address_line1, practice_address_line2, practice_city, practice_state,
    practice_postal_code, practice_country_code, practice_telephone, practice_fax,
    enumeration_date, last_update_date, npi_deactivation_date, npi_reactivation_date,
    auth_last_name, auth_first_name, auth_middle_name,
    auth_name_prefix, auth_name_suffix, auth_credential,
    auth_title_or_position, auth_telephone,
    is_sole_proprietor, is_organization_subpart,
    parent_org_lbn, parent_org_tin, certification_date
) VALUES (
    %s,%s,%s,
    %s,%s,
    %s,%s,%s,%s,%s,%s,%s,
    %s,%s,
    %s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s
)
"""

TAXONOMY_REF_SQL = "INSERT IGNORE INTO taxonomy (taxonomy_code) VALUES (%s)"

PROVIDER_TAXONOMY_SQL = """
INSERT IGNORE INTO provider_taxonomy
    (npi, taxonomy_code, license_number, license_state_code, is_primary, taxonomy_group, slot_order)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

OPI_SQL = """
INSERT IGNORE INTO other_provider_identifier
    (npi, identifier, type_code, state, issuer, slot_order)
VALUES (%s,%s,%s,%s,%s,%s)
"""


def load_main(conn: pymysql.Connection) -> None:
    """
    Load npidata_pfile into:
      provider, taxonomy (ref), provider_taxonomy, other_provider_identifier
    """
    path = os.path.join(DATA_DIR, "data/npidata_pfile_20050523-20260208.csv")
    print(f"  Source: {path}")

    # Build header → column-index map
    with open(path, newline="", encoding="utf-8") as fh:
        col: dict[str, int] = {h: i for i, h in enumerate(csv.reader(fh).__next__())}

    # Precompute repeating-group column indices
    tax_cols: dict[int, dict] = {}
    for s in range(1, 16):
        tax_cols[s] = {
            "code":    col.get(f"Healthcare Provider Taxonomy Code_{s}",        -1),
            "lic_num": col.get(f"Provider License Number_{s}",                  -1),
            "lic_st":  col.get(f"Provider License Number State Code_{s}",       -1),
            "primary": col.get(f"Healthcare Provider Primary Taxonomy Switch_{s}",-1),
            "group":   col.get(f"Healthcare Provider Taxonomy Group_{s}",       -1),
        }

    opi_cols: dict[int, dict] = {}
    for s in range(1, 51):
        opi_cols[s] = {
            "id":    col.get(f"Other Provider Identifier_{s}",           -1),
            "type":  col.get(f"Other Provider Identifier Type Code_{s}", -1),
            "state": col.get(f"Other Provider Identifier State_{s}",     -1),
            "issuer":col.get(f"Other Provider Identifier Issuer_{s}",    -1),
        }

    cursor = conn.cursor()
    known_tax_codes: set[str] = set()

    prov_batch:     list = []
    tax_ref_batch:  list = []
    pt_batch:       list = []
    opi_batch:      list = []
    total:          int  = 0
    start = time.time()

    def flush() -> None:
        if prov_batch:
            cursor.executemany(PROVIDER_SQL, prov_batch)
            prov_batch.clear()
        if tax_ref_batch:
            cursor.executemany(TAXONOMY_REF_SQL, tax_ref_batch)
            tax_ref_batch.clear()
        if pt_batch:
            cursor.executemany(PROVIDER_TAXONOMY_SQL, pt_batch)
            pt_batch.clear()
        if opi_batch:
            cursor.executemany(OPI_SQL, opi_batch)
            opi_batch.clear()
        conn.commit()

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)                    # skip header
        for row in reader:
            npi = v(row, col["NPI"])
            if not npi:
                continue

            total += 1

            # ── provider ────────────────────────────────────────────────────
            prov_batch.append((
                npi,
                v(row, col["Entity Type Code"]),
                v(row, col["Replacement NPI"]),
                # org
                v(row, col["Provider Organization Name (Legal Business Name)"]),
                v(row, col["Employer Identification Number (EIN)"]),
                # individual
                v(row, col["Provider Last Name (Legal Name)"]),
                v(row, col["Provider First Name"]),
                v(row, col["Provider Middle Name"]),
                v(row, col["Provider Name Prefix Text"]),
                v(row, col["Provider Name Suffix Text"]),
                v(row, col["Provider Credential Text"]),
                v(row, col["Provider Sex Code"]),
                # other name
                v(row, col["Provider Other Organization Name"]),
                v(row, col["Provider Other Organization Name Type Code"]),
                v(row, col["Provider Other Last Name"]),
                v(row, col["Provider Other First Name"]),
                v(row, col["Provider Other Middle Name"]),
                v(row, col["Provider Other Name Prefix Text"]),
                v(row, col["Provider Other Name Suffix Text"]),
                v(row, col["Provider Other Credential Text"]),
                v(row, col["Provider Other Last Name Type Code"]),
                # mailing address
                v(row, col["Provider First Line Business Mailing Address"]),
                v(row, col["Provider Second Line Business Mailing Address"]),
                v(row, col["Provider Business Mailing Address City Name"]),
                v(row, col["Provider Business Mailing Address State Name"]),
                v(row, col["Provider Business Mailing Address Postal Code"]),
                v(row, col["Provider Business Mailing Address Country Code (If outside U.S.)"]),
                v(row, col["Provider Business Mailing Address Telephone Number"]),
                v(row, col["Provider Business Mailing Address Fax Number"]),
                # practice location
                v(row, col["Provider First Line Business Practice Location Address"]),
                v(row, col["Provider Second Line Business Practice Location Address"]),
                v(row, col["Provider Business Practice Location Address City Name"]),
                v(row, col["Provider Business Practice Location Address State Name"]),
                v(row, col["Provider Business Practice Location Address Postal Code"]),
                v(row, col["Provider Business Practice Location Address Country Code (If outside U.S.)"]),
                v(row, col["Provider Business Practice Location Address Telephone Number"]),
                v(row, col["Provider Business Practice Location Address Fax Number"]),
                # lifecycle
                parse_date(row[col["Provider Enumeration Date"]]),
                parse_date(row[col["Last Update Date"]]),
                parse_date(row[col["NPI Deactivation Date"]]),
                parse_date(row[col["NPI Reactivation Date"]]),
                # authorized official
                v(row, col["Authorized Official Last Name"]),
                v(row, col["Authorized Official First Name"]),
                v(row, col["Authorized Official Middle Name"]),
                v(row, col["Authorized Official Name Prefix Text"]),
                v(row, col["Authorized Official Name Suffix Text"]),
                v(row, col["Authorized Official Credential Text"]),
                v(row, col["Authorized Official Title or Position"]),
                v(row, col["Authorized Official Telephone Number"]),
                # hierarchy
                v(row, col["Is Sole Proprietor"]),
                v(row, col["Is Organization Subpart"]),
                v(row, col["Parent Organization LBN"]),
                v(row, col["Parent Organization TIN"]),
                parse_date(row[col["Certification Date"]]),
            ))

            # ── provider_taxonomy (1NF: unroll 15 slots) ────────────────────
            for slot, c in tax_cols.items():
                code = v(row, c["code"])
                if not code:
                    continue
                if code not in known_tax_codes:
                    known_tax_codes.add(code)
                    tax_ref_batch.append((code,))
                pt_batch.append((
                    npi, code,
                    v(row, c["lic_num"]),
                    v(row, c["lic_st"]),
                    v(row, c["primary"]),
                    v(row, c["group"]),
                    slot,
                ))

            # ── other_provider_identifier (1NF: unroll 50 slots) ────────────
            for slot, c in opi_cols.items():
                identifier = v(row, c["id"])
                if not identifier:
                    continue
                opi_batch.append((
                    npi, identifier,
                    v(row, c["type"]),
                    v(row, c["state"]),
                    v(row, c["issuer"]),
                    slot,
                ))

            if total % BATCH_SIZE == 0:
                flush()
                progress("provider rows", total, start)

    flush()
    print()
    cursor.close()
    print(f"  Done: {total:,} provider rows  |  {len(known_tax_codes):,} distinct taxonomy codes")


# ── Phase 2: Other names ───────────────────────────────────────────────────────

OTHER_NAME_SQL = """
INSERT IGNORE INTO other_name (npi, other_org_name, other_org_name_type_code)
VALUES (%s,%s,%s)
"""


def load_other_names(conn: pymysql.Connection) -> None:
    path = os.path.join(DATA_DIR, "data/othername_pfile_20050523-20260208.csv")
    print(f"  Source: {path}")
    cursor = conn.cursor()
    batch: list = []
    total = 0
    start = time.time()

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            npi = row[0].strip()
            if not npi:
                continue
            total += 1
            batch.append((npi, row[1].strip() or None, row[2].strip() or None))
            if total % BATCH_SIZE == 0:
                cursor.executemany(OTHER_NAME_SQL, batch)
                conn.commit()
                batch.clear()
                progress("other_name rows", total, start)

    if batch:
        cursor.executemany(OTHER_NAME_SQL, batch)
        conn.commit()
    print()
    cursor.close()
    print(f"  Done: {total:,} other_name rows")


# ── Phase 3: Secondary practice locations ─────────────────────────────────────

SPL_SQL = """
INSERT IGNORE INTO secondary_practice_location
    (npi, address_line1, address_line2, city, state,
     postal_code, country_code, telephone, telephone_ext, fax)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def load_secondary_locations(conn: pymysql.Connection) -> None:
    path = os.path.join(DATA_DIR, "data/pl_pfile_20050523-20260208.csv")
    print(f"  Source: {path}")
    cursor = conn.cursor()
    batch: list = []
    total = 0
    start = time.time()

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            npi = row[0].strip()
            if not npi:
                continue
            total += 1
            batch.append(tuple(c.strip() or None for c in row[:10]))
            if total % BATCH_SIZE == 0:
                cursor.executemany(SPL_SQL, batch)
                conn.commit()
                batch.clear()
                progress("secondary_location rows", total, start)

    if batch:
        cursor.executemany(SPL_SQL, batch)
        conn.commit()
    print()
    cursor.close()
    print(f"  Done: {total:,} secondary_practice_location rows")


# ── Phase 4: Endpoints ─────────────────────────────────────────────────────────

ENDPOINT_SQL = """
INSERT IGNORE INTO endpoint (
    npi, endpoint_type, endpoint_type_description, endpoint_value, affiliation,
    endpoint_description, affiliation_lbn, use_code, use_description,
    other_use_description, content_type, content_description,
    other_content_description, affiliation_address_line1, affiliation_address_line2,
    affiliation_address_city, affiliation_address_state, affiliation_address_country,
    affiliation_address_postal
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def load_endpoints(conn: pymysql.Connection) -> None:
    path = os.path.join(DATA_DIR, "data/endpoint_pfile_20050523-20260208.csv")
    print(f"  Source: {path}")
    cursor = conn.cursor()
    batch: list = []
    total = 0
    start = time.time()

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            npi = row[0].strip()
            if not npi:
                continue
            total += 1
            batch.append(tuple(c.strip() or None for c in row[:19]))
            if total % BATCH_SIZE == 0:
                cursor.executemany(ENDPOINT_SQL, batch)
                conn.commit()
                batch.clear()
                progress("endpoint rows", total, start)

    if batch:
        cursor.executemany(ENDPOINT_SQL, batch)
        conn.commit()
    print()
    cursor.close()
    print(f"  Done: {total:,} endpoint rows")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    wall_start = time.time()
    print("Connecting to MariaDB …")
    try:
        conn = connect_with_retry()
    except pymysql.Error as exc:
        print(f"Connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    # Disable FK checks during bulk load for speed; re-enable after
    with conn.cursor() as cur:
        cur.execute("SET foreign_key_checks = 0")
        cur.execute("SET unique_checks     = 0")
        cur.execute("SET sql_log_bin       = 0")

    print("\n[1/4] Loading providers + taxonomies + other identifiers …")
    load_main(conn)

    print("\n[2/4] Loading other names …")
    load_other_names(conn)

    print("\n[3/4] Loading secondary practice locations …")
    load_secondary_locations(conn)

    print("\n[4/4] Loading endpoints …")
    load_endpoints(conn)

    with conn.cursor() as cur:
        cur.execute("SET foreign_key_checks = 1")
        cur.execute("SET unique_checks     = 1")

    conn.commit()
    conn.close()

    elapsed = time.time() - wall_start
    print(f"\nAll done in {elapsed/60:.1f} minutes.")


if __name__ == "__main__":
    main()
