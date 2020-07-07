import boto3
import click
import json
import logging

from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--bq_table", required=True,
              help="Input BigQuery table containing aggregated metrics")
@click.option("--s3_bucket", required=True, help="S3 bucket for exporting data")
@click.option("--s3_path", required=True, help="S3 path for exporting data")
def main(bq_table, s3_bucket, s3_path):
    """Export User Activity aggregated table to S3"""

    logger.info(f"Starting export from {bq_table} to {s3_bucket}/{s3_path}")

    client = bigquery.Client()

    QUERY = (
        "SELECT FORMAT_DATE('%Y-%m-%d', week_start) AS date, country_name, mau,"
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

    user_activity_metrics_json = json.dumps(user_activity_metrics, indent=4)
    client = boto3.client('s3')
    client.put_object(
        Body=user_activity_metrics_json,
        Bucket=s3_bucket,
        Key=f"{s3_path}/fxhealth.json"
    )

    web_usage_metrics_json = json.dumps(web_usage_metrics, indent=4)
    client = boto3.client('s3')
    client.put_object(
        Body=web_usage_metrics_json,
        Bucket=s3_bucket,
        Key=f"{s3_path}/webusage.json"
    )


if __name__ == "__main__":
    main()
