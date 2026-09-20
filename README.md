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

- `ens_data/sweeps.csv` — everything, one row per reading. Opens in Excel
  (up to Excel's ~1 million row limit — about ten unit-days).
- `ens_data/sweeps/` and `ens_data/hot/` — the same data as Parquet, for
  Python (`pandas.read_parquet`) or DuckDB. The script prints a DuckDB view
  that reads both and removes the small overlap between them.

The first run downloads everything; later runs fetch only what is new.

## Options

| Flag | Effect |
|---|---|
| `--hot` | Also snapshot the current live-database rows (needs `ENS_PG_PASSWORD`) |
| `--csv` | Also write `sweeps.csv` |
| `--no-s3` | Skip the archive; live database only (no AWS credentials needed) |
| `--profile NAME` | AWS CLI profile to use |
| `--dest DIR` | Where to put the files (default `./archive`) |
| `--combine OUT.parquet` | Also write the archive as one Parquet file |

Column meanings and how the data is structured: the *Researcher Data Access
Guide*, from the maintainer.
