import click
import json
import logging

from google.cloud import bigquery, storage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--bq_table", required=True,
              help="Input BigQuery table containing aggregated metrics")
@click.option("--gcs_bucket", required=True, help="GCS bucket for exporting data")
@click.option("--gcs_path", required=True, help="GCS path for exporting data")
def main(bq_table, gcs_bucket, gcs_path):
    """Export User Activity aggregated table to GCS"""

    logger.info(f"Starting export from {bq_table} to {gcs_bucket}/{gcs_path}")

    client = bigquery.Client()

    QUERY = (
        "SELECT FORMAT_DATE('%Y-%m-%d', submission_date) AS date, country_name, mau,"
        " avg_hours_usage_daily, intensity, new_profile_rate, latest_version_ratio,"
        " top_addons, has_addon_ratio, top_locales"
        f" FROM `{bq_table}`"
        )
    query_job = client.query(QUERY)
    rows = query_job.result()

    user_activity_metrics = {}
    web_usage_metrics = {}

    for row in rows:
        if row.country_name not in user_activity_metrics \
                and row.country_name not in web_usage_metrics:
            user_activity_metrics[row.country_name] = []
            web_usage_metrics[row.country_name] = []

        user_activity_metrics[row.country_name].append({
            "date": row.date,
            "metrics": {
                "avg_intensity": float(row.intensity),
                "MAU": row.mau * 100,
                "avg_daily_usage(hours)": float(row.avg_hours_usage_daily),
                "pct_new_user": float(row.new_profile_rate) * 100,
                "pct_latest_version": float(row.latest_version_ratio) * 100
            }
        })
        web_usage_metrics[row.country_name].append({
                "date": row.date,
                "metrics": {
                    "locale": {l["locale"]: l["ratio"] * 100 for l in row["top_locales"]}, # noqa
                    "top10addons": {a["addon_name"]: a["ratio"] * 100 for a in row["top_addons"]}, # noqa
                    "pct_addon": row["has_addon_ratio"] * 100
                }
            })

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)

    user_activity_metrics_json = json.dumps(user_activity_metrics, indent=4)
    blob_fxhealth = bucket.blob(f"{gcs_path}/fxhealth.json")
    blob_fxhealth.upload_from_string(user_activity_metrics_json, content_type="application/json")

    web_usage_metrics_json = json.dumps(web_usage_metrics, indent=4)
    blob_webusage = bucket.blob(f"{gcs_path}/webusage.json")
    blob_webusage.upload_from_string(web_usage_metrics_json, content_type="application/json")


if __name__ == "__main__":
    main()
