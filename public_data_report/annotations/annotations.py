import datetime
import json
import logging

import click
import importlib.resources as pkg_resources
from google.cloud import bigquery, storage

from public_data_report import USER_ACITVITY_COUNTRY_LIST
from public_data_report.annotations import static

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# migrated from https://github.com/mozilla/Fx_Usage_Report/tree/489ca258b14776c01f3021080b2dd686d239dea3/usage_report/annotations # noqa
HARDWARE_ANNOTATIONS_FILE = "annotations_hardware.json"
WEBUSAGE_ANNOTATIONS_FILE = "annotations_webusage.json"
FXHEALTH_ANNOTATIONS_FILE = "annotations_fxhealth.json"

# Annotations that are added to every country
DEFAULT_USAGE_ANNOTATIONS = [
    {
      "annotation": {
        "pct_TP": "FF57",
        "pct_addon": "legacy addons disabled"
      },
      "date": "2017-11-14"
    },
    {
      "annotation": {
        "pct_addon": "data deleted (addons outage)"
      },
      "date": "2019-05-05"
    }
]


def get_fxhealth_annotations(date_to: datetime.datetime) -> str:
    """Return JSON string of annotations for Firefox versions per country."""
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
                            MAX(`mozfun.norm.truncate_version`(build.target.version, "major")
                        ) AS version
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

    fxhealth_annotations = {country: [] for country in USER_ACITVITY_COUNTRY_LIST}
    for row in rows:
        for country in USER_ACITVITY_COUNTRY_LIST:
            fxhealth_annotations[country].append({
                "annotation": {
                    "pct_latest_version": f"FF{row['version']}"
                },
                "date": row["day"]
            })

    return json.dumps(fxhealth_annotations, indent=2)


def get_usage_annotations() -> str:
    """Return JSON string of annotations for Firefox usage per country."""
    usage_annotations = json.loads(pkg_resources.read_text(static, WEBUSAGE_ANNOTATIONS_FILE))
    for country in USER_ACITVITY_COUNTRY_LIST:
        if country not in usage_annotations:
            usage_annotations[country] = []
        usage_annotations[country].extend(DEFAULT_USAGE_ANNOTATIONS)

    return json.dumps(usage_annotations, indent=2, sort_keys=True)


@click.command()
@click.option(
    "--date_to",
    type=click.DateTime(),
    required=True,
    help="End date for producing version release dates",
)
@click.option('--output_bucket', required=True)
@click.option('--output_prefix', required=True)
def main(date_to, output_bucket, output_prefix):
    """Export annotations to GCS"""

    fxhealth_annotations_json = get_fxhealth_annotations(date_to)

    usage_annotations_json = get_usage_annotations()

    hardware_annotations_json = pkg_resources.read_text(static, HARDWARE_ANNOTATIONS_FILE)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(output_bucket)
    for json_data, filename in [
        (fxhealth_annotations_json, FXHEALTH_ANNOTATIONS_FILE),
        (usage_annotations_json, WEBUSAGE_ANNOTATIONS_FILE),
        (hardware_annotations_json, HARDWARE_ANNOTATIONS_FILE),
    ]:
        blob_static_annotation = bucket.blob(f"{output_prefix}/{filename}")
        blob_static_annotation.upload_from_string(json_data, content_type="application/json")
        logging.info(f"Uploaded {blob_static_annotation.size} bytes to {bucket.name}/{blob_static_annotation.name}")


if __name__ == '__main__':
    main()
