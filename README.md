# enav-data-pull

One script that pulls the complete eNav Sampler dataset — the S3 archive plus
the most recent weeks from the live database — into a folder on your
computer, as Parquet and (optionally) a CSV.

It needs two **read-only** credentials from the project maintainer: an AWS
access key for the archive and the database password. Nothing in this
repository grants access on its own.

## Windows (PowerShell)

Once:

```powershell
pip install awscli
aws configure --profile ens-reader        # paste the Access Key ID and Secret Access Key; region ca-central-1
mkdir ~\ens-pull; cd ~\ens-pull
curl.exe -O https://raw.githubusercontent.com/dylancunliffe/enav-data-pull/main/pull_archive.py
curl.exe -O https://raw.githubusercontent.com/dylancunliffe/enav-data-pull/main/requirements.txt
pip install -r requirements.txt
```

Every time:

```powershell
cd ~\ens-pull
$env:ENS_PG_PASSWORD = "the password"; python pull_archive.py --profile ens-reader --hot --csv --dest C:\ens_data
```

## Mac / Linux (Terminal)

Once:

```bash
pip3 install awscli
aws configure --profile ens-reader
mkdir -p ~/ens-pull && cd ~/ens-pull
curl -O https://raw.githubusercontent.com/dylancunliffe/enav-data-pull/main/pull_archive.py
curl -O https://raw.githubusercontent.com/dylancunliffe/enav-data-pull/main/requirements.txt
pip3 install -r requirements.txt
```

Every time:

```bash
cd ~/ens-pull
export ENS_PG_PASSWORD="the password"; python3 pull_archive.py --profile ens-reader --hot --csv --dest ~/ens_data
```

## What you get

- `ens_data/sweeps/` and `ens_data/hot/` — every sweep reading, as Parquet
  (archive and live-database snapshot respectively; they overlap by up to a
  week). The script prints a DuckDB view that reads both and removes the overlap.
- `ens_data/meta/` — `units`, `placements` (where each unit is and has been),
  `current_placements`, `unit_telemetry` (boots, heartbeats, thermal events),
  as Parquet. These exist only in the live database, so `--hot` is required to get them.
- With `--csv`: `sweeps.csv`, `units.csv`, `placements.csv`,
  `current_placements.csv`, `unit_telemetry.csv` in `ens_data/`. Excel opens
  the small ones fine; `sweeps.csv` exceeds Excel's ~1 million rows after
  about ten unit-days and is tens of GB for a fleet — drop `--csv` once the
  fleet is running and work from the Parquet files.

The first run downloads everything; later runs fetch only what is new.

## Options

| Flag | Effect |
|---|---|
| `--hot` | Also snapshot the live database: recent sweeps plus units, placements and telemetry (needs `ENS_PG_PASSWORD`) |
| `--csv` | Also write CSVs of everything pulled |
| `--no-s3` | Skip the archive; live database only (no AWS credentials needed) |
| `--profile NAME` | AWS CLI profile to use |
| `--dest DIR` | Where to put the files (default `./archive`) |
| `--combine OUT.parquet` | Also write the archive as one Parquet file |

Column meanings and how the data is structured: the *Researcher Data Access
Guide*, from the maintainer.
