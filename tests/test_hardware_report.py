from public_data_report.hardware_report import hardware_report

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql import SparkSession


def test_hardware_report_helpers():
    """Test if helper functions work as expected."""
    # Does |get_os_arch| work as expected?
    assert (
        hardware_report.get_os_arch("x86", "Windows_NT", False) == "x86"
    ), "get_os_arch should report an 'x86' for an x86 browser with no is_wow64."
    assert (
        hardware_report.get_os_arch("x86", "Windows_NT", True) == "x86-64"
    ), "get_os_arch should report an 'x86-64' for an x86 browser, on Windows, using Wow64."
    assert (
        hardware_report.get_os_arch("x86", "Darwin", True) == "x86"
    ), "get_os_arch should report an 'x86' for an x86 browser on non Windows platforms."
    assert (
        hardware_report.get_os_arch("x86-64", "Darwin", True) == "x86-64"
    ), "get_os_arch should report an 'x86-64' for an x86-64 browser on non Windows platforms."
    assert (
        hardware_report.get_os_arch("x86-64", "Windows_NT", False) == "x86-64"
    ), "get_os_arch should report an 'x86-64' for an x86-64 browser on Windows platforms."

    # Does |get_gpu_vendor_name| behave correctly?
    assert (
        hardware_report.get_gpu_vendor_name("0x1013") == "Cirrus Logic"
    ), "get_gpu_vendor_name must report the correct vendor name for a known vendor id."
    assert (
        hardware_report.get_gpu_vendor_name("0xfeee") == "Other"
    ), "get_gpu_vendor_name must report 'Other' for an unknown vendor id."

    # Make sure |invert_device_map| works as expected.
    device_data = {"feee": {"family": {"chipset": ["d1d1", "d2d2"]}}}
    inverted_device_data = hardware_report.invert_device_map(device_data)
    assert (
        "0xfeee" in inverted_device_data
    ), "The vendor id must be prefixed with '0x' and be at the root of the map."
    assert (
        len(list(inverted_device_data["0xfeee"].keys())) == 2
    ), "There must be two devices for the '0xfeee' vendor."
    assert all(
        device_id in inverted_device_data["0xfeee"]
        for device_id in ("0xd1d1", "0xd2d2")
    ), "The '0xfeee' vendor must contain the expected devices."
    assert all(
        d in inverted_device_data["0xfeee"]["0xd1d1"] for d in ("family", "chipset")
    ), "The family and chipset data must be reported in the device section."

    # Let's test |get_device_family_chipset|.
    global device_map
    device_map = inverted_device_data
    assert (
        hardware_report.get_device_family_chipset("0xfeee", "0xd1d1", device_map)
        == "family-chipset"
    ), (
        "The family and chipset info must be returned as '<family>-<chipset>' ",
        "for known devices.",
    )
    assert (
        hardware_report.get_device_family_chipset("0xfeee", "0xdeee", device_map)
        == "Other"
    ), "Unknown devices must be reported as 'Other'."
    assert (
        hardware_report.get_device_family_chipset("0xfeeb", "0xdeee", device_map)
        == "Other"
    ), "Unknown families must be reported as 'Other'."


def test_aggregate():
    spark = SparkSession.builder.appName("hardware_report_test").getOrCreate()

    test_schema = StructType(
        [
            StructField("browser_arch", StringType()),
            StructField("os", StringType()),
            StructField("memory_gb", LongType()),
            StructField("is_wow64", BooleanType()),
            StructField("gfx0_vendor_id", StringType()),
            StructField("gfx0_device_id", StringType()),
            StructField("resolution", StringType()),
            StructField("cpu_cores", LongType()),
            StructField("cpu_vendor", StringType()),
            StructField("cpu_speed", DoubleType()),
            StructField("has_flash", BooleanType()),
            StructField("count", LongType()),
        ]
    )

    test_data = [
        [
            "x86-64",
            "Windows_NT-10.0",
            14,
            False,
            "0x10de",
            "0x13c2",
            "1920x1080",
            4,
            "GenuineIntel",
            3600.0,
            True,
            1,
        ],
        [
            "x86-64",
            "Windows_NT-6.2",
            17,
            False,
            "0x1414",
            "0xfefe",
            "1920x1080",
            4,
            "GenuineIntel",
            None,
            False,
            5,
        ],
    ]

    test_df = spark.createDataFrame(test_data, schema=test_schema)

    dicts = test_df.rdd.map(hardware_report.to_dict).collect()
    dicts_expected = [
        {
            "os": "Windows_NT-10.0",
            "arch": "x86-64",
            "cpu_cores": 4,
            "cpu_vendor": "GenuineIntel",
            "cpu_speed": 3.6,
            "resolution": "1920x1080",
            "memory_gb": 14,
            "has_flash": True,
            "os_arch": "x86-64",
            "gfx0_vendor_name": "NVIDIA",
            "gfx0_model": "Maxwell-GM204",
            "count": 1,
        },
        {
            "os": "Windows_NT-6.2",
            "arch": "x86-64",
            "cpu_cores": 4,
            "cpu_vendor": "GenuineIntel",
            "cpu_speed": 1,
            "resolution": "1920x1080",
            "memory_gb": 17,
            "has_flash": False,
            "os_arch": "x86-64",
            "gfx0_vendor_name": "Microsoft Basic",
            "gfx0_model": "Other",
            "count": 5,
        },
    ]

    assert dicts == dicts_expected

    aggregated = hardware_report.aggregate(test_df)
    aggregated_expected = {
        "os": {"Windows_NT-10.0": 1, "Windows_NT-6.2": 5},
        "arch": {"x86-64": 6},
        "cpu_cores": {4: 6},
        "cpu_vendor": {"GenuineIntel": 6},
        "cpu_speed": {3.6: 1, 1: 5},
        "resolution": {"1920x1080": 6},
        "memory_gb": {14: 1, 17: 5},
        "has_flash": {True: 1, False: 5},
        "os_arch": {"x86-64": 6},
        "gfx0_vendor_name": {"NVIDIA": 1, "Microsoft Basic": 5},
        "gfx0_model": {"Maxwell-GM204": 1, "Other": 5},
    }

    assert aggregated == aggregated_expected


def test_collapse_buckets():
    aggregated = {
        "os": {"Windows_NT-10.0": 95, "Windows_NT-6.2": 5},
        "arch": {"x86-64": 100},
        "cpu_cores": {4: 100},
        "cpu_vendor": {"GenuineIntel": 100},
        "cpu_speed": {3.6: 98, 1: 2},
        "resolution": {"1920x1080": 100},
        "memory_gb": {14: 50, 17: 50},
        "has_flash": {True: 1, False: 99},
        "os_arch": {"x86-64": 100},
        "gfx0_vendor_name": {"NVIDIA": 60, "Microsoft Basic": 40},
        "gfx0_model": {"Maxwell-GM204": 95, "Other": 5},
    }
    collapsed_expected = {
        "os": {"Windows_NT-10.0": 0.95, "Other": 0.05},
        "arch": {"x86-64": 1.0},
        "cpu_cores": {"4": 1.0},
        "cpu_vendor": {"GenuineIntel": 1.0},
        "cpu_speed": {"3.6": 0.98, "Other": 0.02},
        "resolution": {"1920x1080": 1.0},
        "memory_gb": {"14": 0.5, "17": 0.5},
        "has_flash": {"Other": 0.01, "False": 0.99},
        "os_arch": {"x86-64": 1.0},
        "gfx0_vendor_name": {"NVIDIA": 0.6, "Microsoft Basic": 0.4},
        "gfx0_model": {"Maxwell-GM204": 0.95, "Other": 0.05},
    }
    collapsed = hardware_report.collapse_buckets(aggregated, 10, 100)

    assert collapsed == collapsed_expected
