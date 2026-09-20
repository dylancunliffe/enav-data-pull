"""eNav-Harvest — pull the entire S3 sweep archive to a local directory.

The archive is a set of Parquet files under `sweeps/YYYY/MM/DD/HHMMSS.parquet`,
one per archive run, each holding rows exactly as they stood in Postgres.
Files never overlap (a row is archived once) and never change after upload,
so the local copy is a plain incremental mirror: anything already present
with the right size is skipped, anything new is downloaded. Re-running after
months only fetches what has been added since.

The directory of Parquet files IS the local database. Query it in place
with DuckDB (recommended -- reads all files as one table without loading
them into memory):

    import duckdb
    con = duckdb.connect()
    con.sql("create view sweeps as select * from read_parquet('archive/sweeps/**/*.parquet')")
    con.sql("select unit_id, count(*) from sweeps group by 1").show()

or, for a slice that fits in memory, pandas:

    import pandas as pd, glob
    df = pd.concat(pd.read_parquet(f) for f in glob.glob("archive/sweeps/**/*.parquet", recursive=True))

`--combine out.parquet` additionally streams every file into one Parquet
file (memory-bounded, one row group at a time) for tools that want a single
file. At fleet scale that file is large; the directory form is usually better.

Only sweep rows OLDER than the hot window (14 days) are in S3. The most
recent ~3 weeks of sweeps live in Postgres, and so -- permanently -- do the
small tables that give the sweeps meaning: `units`, `placements` (where each
unit is and has been) and `unit_telemetry` (boots, heartbeats, thermal
events). `--hot` snapshots all of that using the read-only
`dashboard_readonly` role: sweeps under `hot/`, the small tables under
`meta/`, so one directory holds the complete corpus. Snapshots are replaced
wholesale each run. Hot and archive overlap by up to the 7-day prune buffer, so
deduplicate on (unit_id, client_row_id) when reading both -- the printed
DuckDB view does this.

Credentials: an AWS profile for the read-only IAM user `ens-archive-reader`
(`aws configure --profile ens-reader`), or the default profile if that is
what you have. This script needs only s3:ListBucket and s3:GetObject.
With `--hot`, the Postgres password is read from the environment variable
ENS_PG_PASSWORD (never on the command line). Nothing else is read from the
environment.

Usage:
    python pull_archive.py                       # mirror ens-archive -> ./archive
    python pull_archive.py --dest D:/ens/archive
    python pull_archive.py --profile ens-reader
    python pull_archive.py --bucket ens-archive-mirror   # if primary is unavailable
    python pull_archive.py --combine all_sweeps.parquet
    ENS_PG_PASSWORD=... python pull_archive.py --hot   # archive + current Postgres rows + units/placements/telemetry
"""

import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

DEFAULT_BUCKET = "ens-archive"
PREFIX = "sweeps/"
HOT_DIR = "hot"
META_DIR = "meta"
# Small Postgres-only tables pulled whole with --hot. Never archived to S3; this is
# the only route a data-only user has to them. current_placements is a view.
META_TABLES = ["units", "placements", "unit_telemetry", "current_placements"]

PG_HOST = "aws-0-ca-central-1.pooler.supabase.com"   # session pooler; the direct endpoint is IPv6-only
PG_USER = "dashboard_readonly.tsfoesuxyulcjafdesey"  # role.projectref -- the suffix is required by the pooler
PG_DB = "postgres"
PG_FETCH_ROWS = 100_000

# Column order and types match what archive_sweeps.py writes (PostgREST JSON
# -> pandas -> Parquet): timestamps are ISO-8601 strings, macaddr/uuid are
# strings. Keeping the hot snapshot identical lets DuckDB union the two
# without casts.
SWEEP_SELECT = """
select id, client_row_id, unit_id::text, session_id::text,
       to_json("timestamp")#>>'{}' as "timestamp",
       frequency_hz, rssi, multipath, snr, freq_offset_hz,
       valid_station, clipping, soft_mute, temp_c, receiver_hw_rev, pi_code,
       to_json(inserted_at)#>>'{}' as inserted_at,
       to_json(archived_at)#>>'{}' as archived_at
from sweeps
order by unit_id, client_row_id
"""


def list_archive_objects(s3, bucket: str) -> list[dict]:
    """Every object under the sweeps/ prefix: key, size. Paginated; the
    archive grows by one object per daily run so this stays small."""
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                objects.append({"key": obj["Key"], "size": obj["Size"]})
    objects.sort(key=lambda o: o["key"])
    return objects


def sync(s3, bucket: str, dest: str) -> tuple[int, int, int]:
    """Download every archive object not already present locally at the
    same size. Returns (downloaded, skipped, bytes_downloaded). A partial
    download (wrong size) is re-fetched next run, so an interrupted pull is
    safe to resume."""
    objects = list_archive_objects(s3, bucket)
    if not objects:
        print(f"No archive objects under s3://{bucket}/{PREFIX} -- nothing has been archived yet.")
        return 0, 0, 0

    downloaded = skipped = total_bytes = 0
    for i, obj in enumerate(objects, 1):
        local_path = os.path.join(dest, *obj["key"].split("/"))
        if os.path.isfile(local_path) and os.path.getsize(local_path) == obj["size"]:
            skipped += 1
            continue
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        tmp_path = local_path + ".part"
        print(f"[{i}/{len(objects)}] {obj['key']}  ({obj['size'] / 1e6:.1f} MB)")
        s3.download_file(bucket, obj["key"], tmp_path)
        if os.path.getsize(tmp_path) != obj["size"]:
            os.remove(tmp_path)
            print(f"  size mismatch after download, will retry next run", file=sys.stderr)
            continue
        os.replace(tmp_path, local_path)
        downloaded += 1
        total_bytes += obj["size"]
    return downloaded, skipped, total_bytes


def combine(dest: str, out_path: str) -> int:
    """Stream every local Parquet file into one, without loading the whole
    archive into memory. Returns the row count."""
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    root = os.path.join(dest, PREFIX.rstrip("/"))
    dataset = ds.dataset(root, format="parquet")
    rows = 0
    writer = None
    try:
        for batch in dataset.to_batches():
            if writer is None:
                writer = pq.ParquetWriter(out_path, batch.schema, compression="zstd")
            writer.write_batch(batch)
            rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
    return rows


def snapshot_hot(dest: str, password: str, host: str) -> int:
    """Stream every current Postgres sweeps row into dest/hot/sweeps_<utc>.parquet,
    replacing any previous snapshot. Uses a server-side cursor so memory is
    bounded regardless of row count. Returns the row count."""
    import psycopg2
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datetime import datetime, timezone

    schema = pa.schema([
        ("id", pa.int64()), ("client_row_id", pa.int64()),
        ("unit_id", pa.string()), ("session_id", pa.string()), ("timestamp", pa.string()),
        ("frequency_hz", pa.int64()), ("rssi", pa.float64()), ("multipath", pa.int64()),
        ("snr", pa.float64()), ("freq_offset_hz", pa.float64()),
        ("valid_station", pa.bool_()), ("clipping", pa.bool_()), ("soft_mute", pa.bool_()),
        ("temp_c", pa.float64()), ("receiver_hw_rev", pa.int64()), ("pi_code", pa.int64()),
        ("inserted_at", pa.string()), ("archived_at", pa.string()),
    ])

    hot_dir = os.path.join(dest, HOT_DIR)
    os.makedirs(hot_dir, exist_ok=True)
    out_path = os.path.join(hot_dir, datetime.now(timezone.utc).strftime("sweeps_%Y%m%dT%H%M%SZ.parquet"))
    tmp_path = out_path + ".part"

    conn = psycopg2.connect(host=host, port=5432, dbname=PG_DB, user=PG_USER,
                            password=password, sslmode="require",
                            options="-c timezone=UTC")   # so to_json() emits +00:00, as PostgREST does
    rows = 0
    try:
        with conn.cursor(name="ens_hot_pull") as cur, pq.ParquetWriter(tmp_path, schema, compression="zstd") as writer:
            cur.itersize = PG_FETCH_ROWS
            cur.execute(SWEEP_SELECT)
            while True:
                chunk = cur.fetchmany(PG_FETCH_ROWS)
                if not chunk:
                    break
                cols = list(zip(*chunk))
                table = pa.Table.from_arrays(
                    [pa.array(col, type=field.type) for col, field in zip(cols, schema)], schema=schema)
                writer.write_table(table)
                rows += len(chunk)
                print(f"  hot: {rows:,} rows ...", end="\r")
    finally:
        conn.close()
    print()

    # Replace the previous snapshot only once the new one is complete.
    for name in os.listdir(hot_dir):
        path = os.path.join(hot_dir, name)
        if name.endswith(".parquet") and path != out_path:
            os.remove(path)
    os.replace(tmp_path, out_path)
    return rows


def snapshot_meta(dest: str, password: str, host: str, csv: bool) -> dict:
    """Pull each small table in META_TABLES whole into dest/meta/<table>.parquet
    (and dest/<table>.csv when csv=True). Column types are inferred; timestamps,
    UUIDs, MAC addresses and JSONB become strings so the files are portable.
    Returns {table: row_count}."""
    import json, decimal, datetime, uuid
    import psycopg2
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.csv as pcsv

    def plain(v):
        if v is None or isinstance(v, (bool, int, float, str)):
            return v
        if isinstance(v, (datetime.datetime, datetime.date)):
            return v.isoformat()
        if isinstance(v, decimal.Decimal):
            return float(v)
        if isinstance(v, (dict, list)):
            return json.dumps(v, separators=(",", ":"))
        return str(v)   # uuid, macaddr, anything else

    meta_dir = os.path.join(dest, META_DIR)
    os.makedirs(meta_dir, exist_ok=True)
    conn = psycopg2.connect(host=host, port=5432, dbname=PG_DB, user=PG_USER,
                            password=password, sslmode="require", options="-c timezone=UTC")
    counts = {}
    try:
        for table in META_TABLES:
            with conn.cursor() as cur:
                cur.execute(f"select * from {table}")
                cols = [c[0] for c in cur.description]
                rows = cur.fetchall()
            data = {c: [plain(r[i]) for r in rows] for i, c in enumerate(cols)}
            # all-null columns cannot be inferred; make them strings
            arrays = []
            for c in cols:
                vals = data[c]
                arrays.append(pa.array(vals, type=pa.string()) if all(v is None for v in vals) else pa.array(vals))
            tbl = pa.Table.from_arrays(arrays, names=cols)
            out = os.path.join(meta_dir, f"{table}.parquet")
            pq.write_table(tbl, out + ".part", compression="zstd")
            os.replace(out + ".part", out)
            if csv:
                pcsv.write_csv(tbl, os.path.join(dest, f"{table}.csv"))
            counts[table] = len(rows)
    finally:
        conn.close()
    return counts


def write_csv(dest: str) -> None:
    """Everything under dest (archive + hot), deduplicated on (unit_id, client_row_id),
    as one CSV for Excel-style tools. Excel stops at 1,048,576 rows -- about ten
    unit-days -- so the row count is printed and a warning given past that."""
    import duckdb, glob
    d = dest.replace("\\", "/")
    out = os.path.join(dest, "sweeps.csv").replace("\\", "/")
    # Only patterns that actually match -- before the first archive run there is no sweeps/ tree.
    patterns = [pat for pat in (f"{d}/sweeps/**/*.parquet", f"{d}/hot/*.parquet") if glob.glob(pat, recursive=True)]
    if not patterns:
        print("CSV: nothing pulled yet, nothing to write.")
        return
    con = duckdb.connect()
    con.sql(f"""copy (
        select * exclude(rn) from (
          select *, row_number() over (partition by unit_id, client_row_id order by archived_at nulls last) rn
          from read_parquet({patterns}, union_by_name=true)
        ) where rn = 1 order by unit_id, "timestamp", frequency_hz
      ) to '{out}' (header, delimiter ',')""")
    rows = con.sql(f"select count(*) from read_csv_auto('{out}')").fetchone()[0]
    print(f"CSV: {rows:,} rows -> {out} ({os.path.getsize(out) / 1e6:.1f} MB)")
    if rows > 1_048_576:
        print("  note: more rows than Excel can open (1,048,576). Use Python/DuckDB, or filter by unit and date.")



def main():
    ap = argparse.ArgumentParser(description="Mirror the eNav-Harvest S3 sweep archive locally.")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"source bucket (default {DEFAULT_BUCKET})")
    ap.add_argument("--dest", default="archive", help="local directory (default ./archive)")
    ap.add_argument("--profile", default=None, help="AWS CLI profile to use (default: the default profile)")
    ap.add_argument("--combine", metavar="OUT.parquet", default=None,
                    help="after syncing, also write every file into one Parquet file")
    ap.add_argument("--no-s3", action="store_true",
                    help="skip the S3 archive step (no AWS credentials needed); use with --hot for Postgres only")
    ap.add_argument("--csv", action="store_true",
                    help="also write everything pulled to <dest>/sweeps.csv for Excel and similar (large at fleet scale)")
    ap.add_argument("--hot", action="store_true",
                    help="also snapshot the current Postgres rows (password from ENS_PG_PASSWORD)")
    ap.add_argument("--pg-host", default=PG_HOST, help="Postgres host for --hot (default: session pooler)")
    args = ap.parse_args()

    pg_password = os.environ.get("ENS_PG_PASSWORD", "")
    if args.hot and not pg_password:
        print("--hot needs the dashboard_readonly password in ENS_PG_PASSWORD.", file=sys.stderr)
        sys.exit(1)

    if args.no_s3:
        print("Skipping the S3 archive (--no-s3).")
    else:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        s3 = session.client("s3")

        try:
            downloaded, skipped, total_bytes = sync(s3, args.bucket, args.dest)
        except NoCredentialsError:
            print("No AWS credentials found. Run `aws configure` (or `aws configure --profile ens-reader` "
                  "and pass --profile ens-reader) with the ens-archive-reader access key, or pass --no-s3 "
                  "to pull only the current Postgres rows.", file=sys.stderr)
            sys.exit(1)
        except ClientError as exc:
            print(f"S3 error: {exc}", file=sys.stderr)
            sys.exit(1)

        print(f"\nSynced s3://{args.bucket}/{PREFIX} -> {os.path.abspath(args.dest)}: "
              f"{downloaded} new file(s), {skipped} already present, {total_bytes / 1e9:.2f} GB downloaded.")

    if args.hot:
        print(f"Snapshotting current Postgres rows from {args.pg_host} ...")
        hot_rows = snapshot_hot(args.dest, pg_password, args.pg_host)
        print(f"Hot snapshot: {hot_rows:,} rows -> {os.path.join(os.path.abspath(args.dest), HOT_DIR)}")
        counts = snapshot_meta(args.dest, pg_password, args.pg_host, csv=args.csv)
        print("Units, placements, telemetry -> " + os.path.join(os.path.abspath(args.dest), META_DIR) + ": "
              + ", ".join(f"{t} {n:,}" for t, n in counts.items())
              + ("  (+ CSVs)" if args.csv else ""))

    if args.combine:
        print(f"Combining into {args.combine} ...")
        rows = combine(args.dest, args.combine)
        print(f"Wrote {rows:,} rows to {args.combine} ({os.path.getsize(args.combine) / 1e9:.2f} GB).")

    if args.csv:
        write_csv(args.dest)

    d = args.dest.replace("\\", "/")
    print("\nQuery in place with DuckDB (deduplicated across the archive/hot overlap):")
    print('  con.sql("""create view sweeps as')
    print(f"    select * from read_parquet(['{d}/sweeps/**/*.parquet', '{d}/hot/*.parquet'], union_by_name=true)")
    print('    qualify row_number() over (partition by unit_id, client_row_id order by archived_at nulls last) = 1""")')
    print(f"  Location per unit: read_parquet('{d}/meta/current_placements.parquet'); full history in meta/placements.parquet.")


if __name__ == "__main__":
    main()
