# Firefox Public Data Report ETL
The [Firefox Public Data](https://data.firefox.com) project is a public facing website which tracks various metrics over time and helps the general public understand what kind of data is being tracked by Mozilla and how it is used.

This repository contains the jobs that read the report's aggregated data from BigQuery, format it, and export it as JSON files to GCS for the Hardware, User Activity, and Usage Behavior sections of the report.
It also exports the annotations shown on the report's charts.

The aggregate tables these jobs read from are built by queries in [bigquery-etl](https://github.com/mozilla/bigquery-etl), and the exported JSON files are consumed by [Ensemble Transposer](https://github.com/mozilla/ensemble-transposer) to generate the website.
The jobs are scheduled by the [`firefox_public_data_report`](https://github.com/mozilla/telemetry-airflow/blob/main/dags/firefox_public_data_report.py) Airflow DAG.

## Jobs
All jobs are subcommands of [`public_data_report/cli.py`](public_data_report/cli.py).

### Hardware report
The [hardware report job](public_data_report/hardware_report/hardware_report.py) reads weekly client counts per hardware configuration from [`telemetry_derived.public_data_report_hardware_aggregates_v1`](https://github.com/mozilla/bigquery-etl/blob/main/sql/moz-fx-data-shared-prod/telemetry_derived/public_data_report_hardware_aggregates_v1/query.sql). For each week it:
1. Maps compound dimensions to display values. OS arch is inferred from browser arch, OS, and WOW64. GPU vendor and model names are looked up from vendor and device IDs using [gpu-db](https://github.com/jrmuizel/gpu-db).
2. Groups values with less than 1% of clients into an "Other" bucket (or "<os>-Other" for OS versions) and converts counts to ratios.
3. Writes the result to the week's partition in the output table, [`telemetry_derived.public_data_report_hardware_v1`](https://github.com/mozilla/bigquery-etl/tree/main/sql/moz-fx-data-shared-prod/telemetry_derived/public_data_report_hardware_v1). The schema is defined in bigquery-etl but the table is only populated by this job.

The whole output table is then exported to GCS as `hwsurvey-weekly.json`, along with a dated archive copy (`hwsurvey-weekly-YYYY-MM-DD.json`).
Use `--past_weeks` to backfill multiple weeks and `--dry_run` to skip the GCS upload.

### User Activity
The [user activity job](public_data_report/user_activity/user_activity.py) reads weekly per-country metrics from [`telemetry_derived.public_data_report_user_activity_v1`](https://github.com/mozilla/bigquery-etl/blob/main/sql/moz-fx-data-shared-prod/telemetry_derived/public_data_report_user_activity_v1/query.sql), filters to the countries in the allowlist, and exports two files to GCS:
* `fxhealth.json` (User Activity): MAU, average daily usage hours, average intensity, new profile rate, and latest version ratio.
* `webusage.json` (Usage Behavior): top locales, percentage of users with an add-on, and top add-ons.

See the bigquery-etl query for how each metric is defined.

The countries included in the report are set by `USER_ACITVITY_COUNTRY_LIST` in [`public_data_report/__init__.py`](public_data_report/__init__.py).
The job fails if any country in the list is missing from the input table, so names in the list must match the `country_name` values produced by the query.

### Annotations
The [annotations job](public_data_report/annotations/annotations.py) exports chart annotations to GCS:
* `annotations_fxhealth.json`: Firefox major release dates for every country, queried from `telemetry.buildhub2`.
* `annotations_webusage.json`: [static annotations](public_data_report/annotations/static/annotations_webusage.json) plus default annotations added to every country.
* `annotations_hardware.json`: [static annotations](public_data_report/annotations/static/annotations_hardware.json) copied as-is.

See the [annotations readme](public_data_report/annotations/readme.md) for the file format.

## Running locally
Set up a Python 3.13 virtual environment:
```shell script
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt pytest
pip install -e .
```

The jobs need GCP credentials with access to BigQuery and the output GCS bucket:
```shell script
gcloud auth application-default login
```

Then run a job, for example:
```shell script
python -m public_data_report.cli user_activity \
    --bq_table moz-fx-data-shared-prod.telemetry_derived.public_data_report_user_activity_v1 \
    --gcs_bucket YOUR_BUCKET \
    --gcs_path public-data-report-dev/user_activity

python -m public_data_report.cli hardware_report \
    --date_from 2026-10-19 \
    --input_bq_table moz-fx-data-shared-prod.telemetry_derived.public_data_report_hardware_aggregates_v1 \
    --output_bq_table YOUR_PROJECT.YOUR_DATASET.public_data_report_hardware \
    --gcs_bucket YOUR_BUCKET \
    --gcs_path public-data-report-dev/hardware/ \
    --dry_run

python -m public_data_report.cli annotations \
    --date_to 2026-10-19 \
    --output_bucket YOUR_BUCKET \
    --output_prefix public-data-report-dev/annotations
```

`hardware_report` writes to `--output_bq_table` even with `--dry_run`, so point it at a table you own.

## Development
### Testing
Run the tests and lint from the virtual environment:
```shell script
pytest
flake8 public_data_report tests --max-line-length 100
```

CI runs the same checks in Docker, which can be reproduced with `make build && make test && make lint`.

### Deployment
On every push to `main`, the [GitHub Actions workflow](.github/workflows/build-and-deploy.yml) runs the tests and lint, then builds and pushes the image to `us-docker.pkg.dev/moz-fx-data-artifacts-prod/firefox-public-data-report-etl/firefox-public-data-report-etl:latest`.
