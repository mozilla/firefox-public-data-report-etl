import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Any

import click
import requests
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_aggregation_query(source_table: str):
    """
    Generates a query to get a single row with a column for each hardware dimension.
    Each column contains an array of hardware values and client counts.
    """
    output_fields: Dict[str, Tuple[str]] = {
        "os": ("os",),
        "browser_arch": ("browser_arch",),
        "cpu_cores": ("cpu_cores",),
        "cpu_vendor": ("cpu_vendor",),
        "cpu_speed": ("cpu_speed",),
        "resolution": ("resolution",),
        "memory_gb": ("memory_gb",),
        "has_flash": ("has_flash",),
        "os_arch": ("browser_arch", "os", "is_wow64"),
        "gfx0_vendor_name": ("gfx0_vendor_id",),
        "gfx0_model": ("gfx0_vendor_id", "gfx0_device_id"),
    }

    expr_template = (
        "ARRAY(SELECT AS STRUCT {dimensions}, SUM(client_count) AS client_count "
        f"FROM {source_table} "
        "WHERE date_from = @date_from AND date_to = @date_to "
        "GROUP BY {dimensions}) AS {field}"
    )

    return f"""
    SELECT
      DATE(@date_from) AS date_from,
      DATE(@date_to) AS date_to,
      (
        SELECT
          SUM(client_count)
        FROM {source_table}
        WHERE date_from = @date_from AND date_to = @date_to
      ) AS client_count,
    """ + ",\n".join(
        [
            expr_template.format(field=field, dimensions=", ".join(dimensions))
            for field, dimensions in output_fields.items()
        ]
    )


def load_data(bq_client, input_bq_table, date_from, date_to):
    """Load a set of aggregated metrics for the provided timeframe.

    Returns dictionary containing preaggregated user counts per various dimensions.

    Args:
        date_from: Start date (inclusive)
        date_to: End date (exclusive)
    """
    query = get_aggregation_query(input_bq_table)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date_from", "DATE", date_from),
            bigquery.ScalarQueryParameter("date_to", "DATE", date_to),
        ]
    )
    hardware_by_dimensions_query_job = bq_client.query(query, job_config=job_config)
    hardware_by_dimensions = dict(next(hardware_by_dimensions_query_job.result()))

    if hardware_by_dimensions["client_count"] is None:
        raise ValueError(
            f"No data in {input_bq_table} for {date_from} to {date_to}"
        )

    return hardware_by_dimensions


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
      gpu_vendor_id: A string containing the vendor id.

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
        A string in the format "Device Family Name-Chipset Name"
        or "Other" if unknown.

    """
    if vendor_id not in device_map:
        return "Other"

    if device_id not in device_map[vendor_id]:
        return "Other"

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


def transform_dimensions(
    hardware_by_dimensions: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Dict[str, int]]:
    """Transform compound dimensions into the desired values.

    e.g. convert gfx0_vendor_id and gfx0_device_id into gfx0_model

    Returns a dict of {dimension_name: {value: client_count}}
    """
    device_map = build_device_map()

    untransformed_dimensions = [
        "os",
        "browser_arch",
        "cpu_cores",
        "cpu_vendor",
        "resolution",
        "memory_gb",
        "has_flash",
        "cpu_speed",
    ]

    def untransformed(dim):
        return {
            dim_value[dim]: dim_value["client_count"]
            for dim_value in hardware_by_dimensions[dim]
        }

    os_arch_count = defaultdict(int)
    gfx_vendor_count = defaultdict(int)
    gfx_model_count = defaultdict(int)

    for os_arch in hardware_by_dimensions["os_arch"]:
        os_arch_count[
            get_os_arch(os_arch["browser_arch"], os_arch["os"], os_arch["is_wow64"])
        ] += os_arch["client_count"]

    for gfx_vendor in hardware_by_dimensions["gfx0_vendor_name"]:
        gfx_vendor_count[
            get_gpu_vendor_name(gfx_vendor["gfx0_vendor_id"])
        ] += gfx_vendor["client_count"]

    for gfx_model in hardware_by_dimensions["gfx0_model"]:
        gfx_model_count[
            get_device_family_chipset(
                gfx_model["gfx0_vendor_id"], gfx_model["gfx0_device_id"], device_map
            )
        ] += gfx_model["client_count"]

    return {
        **{dim: untransformed(dim) for dim in untransformed_dimensions},
        "os_arch": dict(os_arch_count),
        "gfx0_vendor_name": dict(gfx_vendor_count),
        "gfx0_model": dict(gfx_model_count),
    }


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
                collapsed_counts[k] = collapsed_counts.get(k, 0) + v
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


def flatten_aggregates(aggregates: List[Dict]):
    keys_translation = {
        "browser_arch": "browserArch_",
        "cpu_cores": "cpuCores_",
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
        for metric, values in aggregate.items():
            if metric in keys_translation:
                for kv_pair in values:
                    flattened[keys_translation[metric] + str(kv_pair["key"])] = kv_pair["value"]
        flattened["date"] = aggregate["date_from"].isoformat()
        flattened_list.append(flattened)
    return flattened_list


def upload_data_gcs(
    output_data: List[Dict], gcs_bucket: str, gcs_path: str, dryrun: bool
):
    aggregates_flattened = sorted(
        flatten_aggregates(output_data), key=lambda a: a["date"], reverse=True
    )
    aggregates_flattened_json = json.dumps(aggregates_flattened, indent=4)

    with open("hwsurvey-weekly.json", "w") as output_file:
        output_file.write(aggregates_flattened_json)

    # Store dataset to GCS. Since GCS doesn't support symlinks, make
    # two copies of the file: one will always contain the latest data,
    # the other for archiving.
    archived_file_copy = f"hwsurvey-weekly-{datetime.today().strftime('%Y-%m-%d')}.json"

    if dryrun:
        logger.info(f"DRYRUN - Skipping upload to gcs bucket: {gcs_bucket}, path: {gcs_path}")
    else:
        logger.info(f"Uploading data to gcs bucket: {gcs_bucket}, path: {gcs_path}")

        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket)

        blob_archive = bucket.blob(gcs_path + archived_file_copy)
        blob_archive.upload_from_filename("hwsurvey-weekly.json")

        blob_latest = bucket.blob(gcs_path + "hwsurvey-weekly.json")
        blob_latest.upload_from_filename("hwsurvey-weekly.json")


@click.command()
@click.option(
    "--date_from",
    type=click.DateTime(),
    required=True,
    help="Aggregation start date (e.g. yyyy-mm-dd)",
)
@click.option(
    "--input_bq_table",
    required=True,
    help="BigQuery table containing the per client input data in project.dataset.table format",
)
@click.option(
    "--output_bq_table",
    required=True,
    help="BigQuery table to write transformed aggregated data to in project.dataset.table format",
)
@click.option("--gcs_bucket", required=True, help="GCS bucket for storing output data")
@click.option("--gcs_path", required=True, help="GCS path for storing output data")
@click.option(
    "--past_weeks",
    type=int,
    default=0,
    help="Number of past weeks to include (useful for backfills)",
)
@click.option(
    "--dry_run",
    "--dryrun",
    default=False,
    is_flag=True,
    help="If dry run is set, data will not be uploaded to GCS",
)
def main(date_from, input_bq_table, output_bq_table, gcs_bucket, gcs_path, past_weeks, dry_run):
    """Generate weekly hardware report for [date_from, date_from + 7) timeframe.

    Aggregates are incrementally inserted to provided BigQuery table,
    finally table is exported to JSON and copied to GCS.
    """
    date_from = date_from.date()
    logger.info(f"Starting, date_from={date_from}, past_weeks={past_weeks}")

    bq_client = bigquery.Client()

    for batch_number in range(0, past_weeks + 1):
        # generate aggregates
        batch_date_from = date_from - timedelta(weeks=1 * batch_number)
        batch_date_to = batch_date_from + timedelta(days=7)
        logger.info(
            f"Running batch {batch_number + 1}/{past_weeks + 1}, "
            f"timeframe: [{batch_date_from}, {batch_date_to})"
        )
        hardware_by_dimensions = load_data(
            bq_client, input_bq_table, batch_date_from, batch_date_to
        )

        transformed = transform_dimensions(hardware_by_dimensions)

        # Collapse together groups that count less than 1% of our samples.
        threshold_to_collapse = int(hardware_by_dimensions["client_count"] * 0.01)

        percentages = collapse_buckets(
            transformed, threshold_to_collapse, hardware_by_dimensions["client_count"]
        )

        # convert to bigquery row format
        for dimension in percentages:
            kv_array = []
            for value, count in percentages[dimension].items():
                kv_array.append({"key": value, "value": count})
            percentages[dimension] = sorted(kv_array, key=lambda x: x["key"])

        percentages["date_from"] = batch_date_from.isoformat()
        percentages["date_to"] = batch_date_to.isoformat()

        # save to BQ
        load_config = bigquery.LoadJobConfig()
        load_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
        bq_client.load_table_from_json(
            json_rows=[percentages],
            destination=f"{output_bq_table}${batch_date_from:%Y%m%d}",
            job_config=load_config,
        ).result()

    output_data = [
        dict(row) for row in
        bq_client.query(f"SELECT * FROM {output_bq_table} ORDER BY date_from").result()
    ]

    upload_data_gcs(output_data, gcs_bucket, gcs_path, dry_run)


if __name__ == "__main__":
    main()
