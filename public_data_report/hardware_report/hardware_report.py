import boto3
import click
from datetime import datetime, timedelta
import json
import logging
import requests

from google.cloud import bigquery

from pyspark.sql import SparkSession, Row


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_data(spark, date_from, date_to):
    """Load a set of metrics per client for the provided timeframe.
  
    Returns Spark dataframe with a row per client.

    Args:
        date_from: Start date (inclusive)
        date_to: End date (exclusive)
    """
    bq = bigquery.Client()

    query = """
  WITH
    rank_per_client AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp DESC) AS rn
      FROM
        `moz-fx-data-shared-prod.telemetry_stable.main_v4`
      WHERE
        DATE(submission_timestamp) >= @date_from
        AND DATE(submission_timestamp) < @date_to
    ),
    latest_per_client_all AS (
      SELECT
        *
      FROM
        rank_per_client
      WHERE
        rn=1
    ),
    latest_per_client AS (
      SELECT
        environment.build.architecture AS browser_arch,
        COALESCE(environment.system.os.name,
            'Other') AS os_name,
        COALESCE(
            IF (environment.system.os.name='Linux',
                REGEXP_EXTRACT(environment.system.os.version, r"^[0-9]+\.[0-9]+"),
                environment.system.os.version),
            'Other') AS os_version,
        environment.system.memory_mb,
        coalesce(environment.system.is_wow64, FALSE) AS is_wow64,
        IF (ARRAY_LENGTH(environment.system.gfx.adapters)>0,
            environment.system.gfx.adapters[OFFSET(0)].vendor_id,
            NULL) AS gfx0_vendor_id,
        IF (ARRAY_LENGTH(environment.system.gfx.adapters)>0,
            environment.system.gfx.adapters[OFFSET(0)].device_id,
            NULL) AS gfx0_device_id,
        IF (ARRAY_LENGTH(environment.system.gfx.monitors)>0,
            environment.system.gfx.monitors[OFFSET(0)].screen_width,
            0) AS screen_width,
        IF (ARRAY_LENGTH(environment.system.gfx.monitors)>0,
            environment.system.gfx.monitors[OFFSET(0)].screen_height,
            0) AS screen_height,
        environment.system.cpu.cores AS cpu_cores,
        environment.system.cpu.vendor AS cpu_vendor,
        environment.system.cpu.speed_m_hz AS cpu_speed,
        'Shockwave Flash' IN (SELECT name FROM UNNEST(environment.addons.active_plugins)) AS has_flash
      FROM
        latest_per_client_all
    ),
    transformed AS (
      SELECT
        browser_arch,
        CONCAT(os_name, '-', os_version) AS os,
        COALESCE(SAFE_CAST(ROUND(memory_mb / 1024.0) AS INT64), 0) AS memory_gb,
        is_wow64,
        gfx0_vendor_id,
        gfx0_device_id,
        CONCAT(CAST(screen_width AS STRING), 'x', CAST(screen_height AS STRING)) AS resolution,
        cpu_cores,
        cpu_vendor,
        cpu_speed,
        has_flash
      FROM
        latest_per_client
    ),
    by_dimensions AS (
      SELECT
        *, 
        count(*) AS count
      FROM
        transformed
      GROUP BY
        browser_arch,
        os,
        memory_gb,
        is_wow64,
        gfx0_vendor_id,
        gfx0_device_id,
        resolution,
        cpu_cores,
        cpu_vendor,
        cpu_speed,
        has_flash
    )
    select * from by_dimensions 
  """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date_from", "DATE", date_from),
            bigquery.ScalarQueryParameter("date_to", "DATE", date_to),
        ]
    )
    hardware_by_dimensions_query_job = bq.query(query, job_config=job_config)
    hardware_by_dimensions_query_job.result()

    hardware_by_dimensions_df = (
        spark.read.format("bigquery")
        .option("project", hardware_by_dimensions_query_job.destination.project)
        .option("dataset", hardware_by_dimensions_query_job.destination.dataset_id)
        .option("table", hardware_by_dimensions_query_job.destination.table_id)
        .load()
    )

    return hardware_by_dimensions_df


def get_os_arch(browser_arch, os_name, is_wow64):
    """Infer the OS arch from environment data.

    Args:
        browser_arch: the browser architecture string (either "x86" or "x86-64").
        os_name: the operating system name.
        is_wow64: on Windows, indicates if the browser process is running under WOW64.

    Returns:
        'x86' if the underlying OS is 32bit, 'x86-64' if it's a 64bit OS.

    """
    is_64bit_browser = browser_arch == "x86-64"
    # If it's a 64bit browser build, then we're on a 64bit system.
    if is_64bit_browser:
        return "x86-64"

    is_windows = os_name == "Windows_NT"
    # If we're on Windows, with a 32bit browser build, and |isWow64 = true|,
    # then we're on a 64 bit system.
    if is_windows and is_wow64:
        return "x86-64"

    # Otherwise we're probably on a 32 bit system.
    return "x86"


def get_gpu_vendor_name(gpu_vendor_id):
    """Get the string name matching the provided vendor id.

    Args:
      id: A string containing the vendor id.

    Returns:
      A string containing the vendor name or "Other" if
      unknown.

    """
    GPU_VENDOR_MAP = {
        "0x1013": "Cirrus Logic",
        "0x1002": "AMD",
        "0x8086": "Intel",
        "Intel Open Source Technology Center": "Intel",
        "0x5333": "S3 Graphics",
        "0x1039": "SIS",
        "0x1106": "VIA",
        "0x10de": "NVIDIA",
        "0x102b": "Matrox",
        "0x15ad": "VMWare",
        "0x80ee": "Oracle VirtualBox",
        "0x1414": "Microsoft Basic",
    }
    return GPU_VENDOR_MAP.get(gpu_vendor_id, "Other")


def get_device_family_chipset(vendor_id, device_id, device_map):
    """Get the family and chipset strings given the vendor and device ids.

    Args:
        vendor_id: a string representing the vendor id (e.g. '0xabcd').
        device_id: a string representing the device id (e.g. '0xbcde').

    Returns:
        A string in the format "Device Family Name-Chipset Name".

    """
    if vendor_id not in device_map:
        return "Unknown"

    if device_id not in device_map[vendor_id]:
        return "Unknown"

    return "-".join(device_map[vendor_id][device_id])


def invert_device_map(m):
    """Inverts a GPU device map fetched from the jrmuizel's Github repo.

    The layout of the fetched GPU map layout is:
        Vendor ID -> Device Family -> Chipset -> [Device IDs]
    We should convert it to:
        Vendor ID -> Device ID -> [Device Family, Chipset]

    """
    device_id_map = {}
    for vendor, u in m.items():
        device_id_map["0x" + vendor] = {}
        for family, v in u.items():
            for chipset, ids in v.items():
                device_id_map["0x" + vendor].update(
                    {("0x" + gfx_id): [family, chipset] for gfx_id in ids}
                )
    return device_id_map


def fetch_json(uri):
    """Perform an HTTP GET on the given uri, return the results as json.

    If there is an error fetching the data, raise an exception.

    Args:
        uri: the string URI to fetch.

    Returns:
        A JSON object with the response.

    """
    data = requests.get(uri)
    # Raise an exception if the fetch failed.
    data.raise_for_status()
    return data.json()


def build_device_map():
    """Build a dictionary that will help us map vendor/device ids to device families."""
    intel_raw = fetch_json("https://github.com/jrmuizel/gpu-db/raw/master/intel.json")
    nvidia_raw = fetch_json("https://github.com/jrmuizel/gpu-db/raw/master/nvidia.json")
    amd_raw = fetch_json("https://github.com/jrmuizel/gpu-db/raw/master/amd.json")

    device_map = {}
    device_map.update(invert_device_map(intel_raw))
    device_map.update(invert_device_map(nvidia_raw))
    device_map.update(invert_device_map(amd_raw))

    return device_map


DEVICE_MAP = build_device_map()


def to_dict(row):
    cpu_speed = 1 if row["cpu_speed"] is None else round(row["cpu_speed"] / 1000.0, 1)
    return {
        "os": row["os"],
        "arch": row["browser_arch"],
        "cpu_cores": row["cpu_cores"],
        "cpu_vendor": row["cpu_vendor"],
        "cpu_speed": cpu_speed,
        "resolution": row["resolution"],
        "memory_gb": row["memory_gb"],
        "has_flash": row["has_flash"],
        "os_arch": get_os_arch(row["browser_arch"], row["os"], row["is_wow64"]),
        "gfx0_vendor_name": get_gpu_vendor_name(row["gfx0_vendor_id"]),
        "gfx0_model": get_device_family_chipset(
            row["gfx0_vendor_id"], row["gfx0_device_id"], DEVICE_MAP
        ),
    }


def add_counts(dict):
    return {k: {v: 1} for k, v in dict.items()}


def combine(acc, row):
    for metric, values in row.items():
        acc_metric = acc.get(metric, {})
        for value, count in values.items():
            acc_metric[value] = acc_metric.get(value, 0) + count
        acc[metric] = acc_metric
    return acc


def aggregate(hardware_by_dimensions_df):
    return hardware_by_dimensions_df.rdd.map(to_dict).map(add_counts).fold({}, combine)


def collapse_buckets(aggregated_data, count_threshold, sample_count):
    OTHER_KEY = "Other"
    collapsed_groups = {}
    for dimension, counts in aggregated_data.items():
        collapsed_counts = {}
        for k, v in counts.items():
            if dimension == "resolution" and k == "0x0":
                collapsed_counts[OTHER_KEY] = collapsed_counts.get(OTHER_KEY, 0) + v
            elif v < count_threshold:
                if dimension == "os":
                    # create generic key per os name
                    [os, ver] = k.split("-", 1)
                    generic_os_key = os + "-" + "Other"
                    collapsed_counts[generic_os_key] = (
                        collapsed_counts.get(generic_os_key, 0) + v
                    )
                else:
                    collapsed_counts[OTHER_KEY] = collapsed_counts.get(OTHER_KEY, 0) + v
            else:
                collapsed_counts[k] = v
        if dimension == "os":
            # The previous grouping might have created additional os groups.
            # Let's check again.
            final_collapsed = {}
            for k, v in collapsed_counts.items():
                if v < count_threshold:
                    final_collapsed[OTHER_KEY] = final_collapsed.get(OTHER_KEY, 0) + v
                else:
                    final_collapsed[k] = v
            collapsed_counts = final_collapsed
        collapsed_groups[dimension] = collapsed_counts

    ratios = {}
    for dimension, counts in collapsed_groups.items():
        ratios[dimension] = {
            str(metric): count / float(sample_count) for metric, count in counts.items()
        }

    return ratios


def flatten_aggregates(aggregates):
    keys_translation = {
        "arch": "browserArch_",
        "cpu_cores": "cpuCores_",
        # "cpu_cores_speed": "cpuCoresSpeed_",
        "cpu_vendor": "cpuVendor_",
        "cpu_speed": "cpuSpeed_",
        "gfx0_vendor_name": "gpuVendor_",
        "gfx0_model": "gpuModel_",
        "resolution": "resolution_",
        "memory_gb": "ram_",
        "os": "osName_",
        "os_arch": "osArch_",
        "has_flash": "hasFlash_",
    }
    flattened_list = []
    for aggregate in aggregates:
        flattened = {}
        for metric, values in json.loads(aggregate).items():
            if metric in keys_translation:
                for k, v in values.items():
                    flattened[keys_translation[metric] + k] = v
        flattened["date"] = json.loads(aggregate)["date"]
        flattened_list.append(flattened)
    return flattened_list


def upload_data_s3(spark, bq_table_name, s3_bucket, s3_path):
    hardware_aggregates_df = (
        spark.read.format("bigquery").option("table", bq_table_name).load()
    )

    map_fields = [
        "arch",
        "cpu_cores",
        "cpu_vendor",
        "cpu_speed",
        "gfx0_vendor_name",
        "gfx0_model",
        "resolution",
        "memory_gb",
        "os",
        "os_arch",
        "has_flash",
    ]
    select_exprs = ["date_from AS date"]
    for field in map_fields:
        select_exprs.append(f"MAP_FROM_ENTRIES({field}.key_value) AS {field}")
    aggregates = hardware_aggregates_df.selectExpr(select_exprs).toJSON().collect()

    aggregates_flattened = sorted(
        flatten_aggregates(aggregates), key=lambda a: a["date"], reverse=True
    )
    aggregates_flattened_json = json.dumps(aggregates_flattened, indent=4)

    with open("hwsurvey-weekly.json", "w") as output_file:
        output_file.write(aggregates_flattened_json)

    # Store dataset to S3. Since S3 doesn't support symlinks, make
    # two copies of the file: one will always contain the latest data,
    # the other for archiving.
    archived_file_copy = f"hwsurvey-weekly-{datetime.today().strftime('%Y-%m-%d')}.json"

    logger.info(f"Uploading data to s3 bucket: {s3_bucket}, path: {s3_path}")
    client = boto3.client("s3", "us-west-2")
    transfer = boto3.s3.transfer.S3Transfer(client)
    transfer.upload_file(
        "hwsurvey-weekly.json", s3_bucket, s3_path + archived_file_copy
    )
    transfer.upload_file(
        "hwsurvey-weekly.json", s3_bucket, s3_path + "hwsurvey-weekly.json"
    )


date_type = click.DateTime()


@click.command()
@click.option(
    "--date_from",
    type=date_type,
    required=True,
    help="Aggregation start date (e.g. yyyy-mm-dd)",
)
@click.option("--bq_table", required=True, help="Output BigQuery table")
@click.option("--s3_bucket", required=True, help="S3 bucket for storing data")
@click.option("--s3_path", required=True, help="S3 path for storing data")
@click.option(
    "--past_weeks",
    type=int,
    default=0,
    help="Number of past weeks to include (useful for backfills)",
)
def main(date_from, bq_table, s3_bucket, s3_path, past_weeks):
    """Generate weekly hardware report for [date_from, date_from_7) timeframe
  
  Aggregates are incrementally inserted to provided BigQuery table,
  finally table is exported to JSON and copied to S3.
  """
    date_from = date_from.date()
    logger.info(f"Starting, date_from={date_from}, past_weeks={past_weeks}")
    spark = SparkSession.builder.appName("hardware_report_dashboard").getOrCreate()

    for batch_number in range(0, past_weeks + 1):
        # generate aggregates
        batch_date_from = date_from - timedelta(weeks=1 * batch_number)
        batch_date_to = batch_date_from + timedelta(days=7)
        logger.info(
            f"Running batch {batch_number}/{past_weeks}, timeframe: [{batch_date_from}, {batch_date_to})"
        )
        hardware_by_dimensions_df = load_data(spark, batch_date_from, batch_date_to)

        aggregated = aggregate(hardware_by_dimensions_df)

        # Collapse together groups that count less than 1% of our samples.
        sample_count = hardware_by_dimensions_df.count()
        threshold_to_collapse = int(sample_count * 0.01)

        aggregates = collapse_buckets(aggregated, threshold_to_collapse, sample_count)
        aggregates["date_from"] = batch_date_from
        aggregates["date_to"] = batch_date_to

        # save to BQ
        aggregates_df = spark.createDataFrame(Row(**x) for x in [aggregates])
        aggregates_df.write.format("bigquery").option("table", bq_table).option(
            "temporaryGcsBucket", "spark-bigquery-dev-test"
        ).mode("append").save()

    upload_data_s3(spark, bq_table, s3_bucket, s3_path)

    spark.stop()


if __name__ == "__main__":
    main()
