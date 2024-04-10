import json
import logging

import click
from google.cloud import bigquery, storage

from public_data_report import USER_ACITVITY_COUNTRY_LIST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--bq_table",
    required=True,
    help="Input BigQuery table containing aggregated metrics",
)
@click.option("--gcs_bucket", required=True, help="GCS bucket for exporting data")
@click.option("--gcs_path", required=True, help="GCS path for exporting data")
def main(bq_table, gcs_bucket, gcs_path):
    """Export User Activity aggregated table to GCS"""

    logger.info(f"Starting export from {bq_table} to {gcs_bucket}/{gcs_path}")

    client = bigquery.Client()

    QUERY = f"""
        SELECT FORMAT_DATE('%Y-%m-%d', submission_date) AS date, country_name, mau,
        avg_hours_usage_daily, intensity, new_profile_rate, latest_version_ratio,
        top_addons, has_addon_ratio, top_locales
        FROM `{bq_table}`
        WHERE country_name IN (
            {", ".join(map(lambda x: f"'{x}'", USER_ACITVITY_COUNTRY_LIST))}
        )
    """

    query_job = client.query(QUERY)
    rows = query_job.result()

    user_activity_metrics = {}
    web_usage_metrics = {}

    for row in rows:
        if (
            row.country_name not in user_activity_metrics
            and row.country_name not in web_usage_metrics
        ):
            user_activity_metrics[row.country_name] = []
            web_usage_metrics[row.country_name] = []

        user_activity_metrics[row.country_name].append(
            {
                "date": row.date,
                "metrics": {
                    "avg_intensity": float(row.intensity),
                    "MAU": row.mau * 100,
                    "avg_daily_usage(hours)": float(row.avg_hours_usage_daily),
                    "pct_new_user": float(row.new_profile_rate) * 100,
                    "pct_latest_version": float(row.latest_version_ratio) * 100,
                },
            }
        )
        web_usage_metrics[row.country_name].append(
            {
                "date": row.date,
                "metrics": {
                    "locale": {
                        locale["locale"]: locale["ratio"] * 100 for locale in row["top_locales"]
                    },
                    "top10addons": {
                        addon["addon_name"]: addon["ratio"] * 100 for addon in row["top_addons"]
                    },
                    "pct_addon": row["has_addon_ratio"] * 100,
                },
            }
        )

    # validate country list
    country_allowlist = set(USER_ACITVITY_COUNTRY_LIST)
    missing_countries = country_allowlist - (
        set(web_usage_metrics.keys()) | web_usage_metrics.keys()
    )
    unexpected_countries = (
        set(web_usage_metrics.keys()) | web_usage_metrics.keys()
    ) - country_allowlist
    errors = []
    if len(missing_countries) > 0:
        errors.append(f"Expected countries missing: {missing_countries}")
    if len(unexpected_countries) > 0:
        errors.append(
            f"Countries not in allowlist but included in output: {unexpected_countries}"
        )
    if len(errors) > 0:
        raise RuntimeError(f"Invalid countries in output: {', '.join(errors)}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)

    for json_data, filename in [
        (user_activity_metrics, "fxhealth.json"),
        (web_usage_metrics, "webusage.json"),
    ]:
        json.dumps(user_activity_metrics, indent=4)
        blob = bucket.blob(f"{gcs_path}/{filename}")
        blob.upload_from_string(
            json.dumps(json_data, indent=4), content_type="application/json"
        )
        logging.info(f"Uploaded {blob.size} bytes to {bucket.name}/{blob.name}")


if __name__ == "__main__":
    main()
