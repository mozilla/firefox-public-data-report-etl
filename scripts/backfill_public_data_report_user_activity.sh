#!/usr/bin/env bash
set -o xtrace

# This script is used to bootstrap aggregate user_activity table
# Weekly query is maintaned in https://github.com/mozilla/bigquery-etl/blob/master/sql/telemetry_derived/public_data_report_user_activity_v1/query.sql

cat public_data_report_user_activity.sql | bq query \
    --destination_table=moz-fx-data-shared-prod:telemetry_derived.public_data_report_user_activity_v1 \
    --use_legacy_sql=false \
    --replace=true \
    --project_id=moz-fx-data-derived-datasets
