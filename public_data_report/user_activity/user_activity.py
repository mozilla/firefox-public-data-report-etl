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
        " avg_hours_usage_daily, intensity, new_profile_rate, latest_version_ratio"
        f" FROM `{bq_table}`"
        )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    out = {}

    for row in rows:
        if row.country_name not in out:
            out[row.country_name] = []

        out[row.country_name].append({
            "date": row.date,
            "metrics": {
                "avg_intensity": float(row.intensity),
                "MAU": row.mau,
                "avg_daily_usage(hours)": float(row.avg_hours_usage_daily),
                "pct_new_user": float(row.new_profile_rate),
                "pct_latest_version": float(row.latest_version_ratio)
            }
        })

    out_json = json.dumps(out, indent=4)
    client = boto3.client('s3')
    client.put_object(Body=out_json, Bucket=s3_bucket, Key=s3_path)


if __name__ == "__main__":
    main()
