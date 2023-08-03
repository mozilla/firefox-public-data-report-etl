import click
import importlib.resources as pkg_resources
from . import static
import json
import logging

from google.cloud import bigquery, storage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUNTRIES = [
    'Worldwide',
    'United States',
    'Germany',
    'France',
    'India',
    'Brazil',
    'China',
    'Indonesia',
    'Russia',
    'Italy',
    'Poland'
]

# migrated from https://github.com/mozilla/Fx_Usage_Report/tree/489ca258b14776c01f3021080b2dd686d239dea3/usage_report/annotations # noqa
STATIC_ANNOTATIONS = [
    "annotations_webusage.json",
    "annotations_hardware.json"
]

date_type = click.DateTime()


@click.command()
@click.option(
    "--date_to",
    type=date_type,
    required=True,
    help="End date for producing version release dates",
)
@click.option('--output_bucket', required=True)
@click.option('--output_prefix', required=True)
def main(date_to, output_bucket, output_prefix):
    """Export annotations to GCS"""

    client = bigquery.Client()

    QUERY = f"""WITH days AS (
                    SELECT
                        day
                    FROM
                        UNNEST(
                            GENERATE_DATE_ARRAY(
                                '2018-12-31',
                                '{date_to.date()}',
                                INTERVAL 7 DAY)
                        ) AS day
                ),
                latest_version_per_day AS (
                    SELECT
                        day,
                        MAX(`mozfun.norm.truncate_version`(build.target.version, "major")) AS version
                    FROM
                        `moz-fx-data-shared-prod.telemetry.buildhub2`
                    JOIN
                        days
                    ON
                        DATE(build.build.date) <= day
                    WHERE
                        build.target.channel = 'release'
                        AND DATE(build.build.date) >= '2018-10-31'
                    GROUP BY
                        day
                )
                SELECT
                    MIN(FORMAT_DATE('%Y-%m-%d', day)) AS day,
                    version
                FROM
                    latest_version_per_day
                GROUP BY
                    version
                ORDER BY
                    day DESC"""

    query_job = client.query(QUERY)
    rows = query_job.result()

    fxhealth_annotations = {country: [] for country in COUNTRIES}

    for row in rows:
        for country in COUNTRIES:
            fxhealth_annotations[country].append({
                "annotation": {
                    "pct_latest_version": f"FF{row['version']}"
                },
                "date": row['day']
            })

    fxhealth_annotations_json = json.dumps(fxhealth_annotations, indent=4)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(output_bucket)
    blob_annotation = bucket.blob(f"{output_prefix}/annotations_fxhealth.json")
    blob_annotation.upload_from_string(fxhealth_annotations_json, content_type="application/json")

    for static_annotation_file in STATIC_ANNOTATIONS:
        data = pkg_resources.read_text(static, static_annotation_file)
        blob_static_annotation = bucket.blob(f"{output_prefix}/{static_annotation_file}")
        blob_static_annotation.upload_from_string(data)


if __name__ == '__main__':
    main()
