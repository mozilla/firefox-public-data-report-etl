#!/usr/bin/env bash
set -o xtrace

# This script is used to bootstrap aggregate user_activity table (weekly query is maintaned in bigquery-etl repository)

cat public_data_report_user_activity.sql | bq query \
    --destination_table=moz-fx-data-shared-prod:analysis.public_data_report_user_activity \
    --use_legacy_sql=false \
    --replace=true \
    --project_id=spark-bigquery-dev
