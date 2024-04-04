from unittest import mock

from public_data_report.hardware_report import hardware_report

DEVICE_MAP_SAMPLE = {
  "0x10de": {
    "0x13c1": [
      "Maxwell",
      "GM204"
    ],
    "0x13c2": [
      "Maxwell",
      "GM204"
    ],
    "0x13d7": [
      "Maxwell",
      "GM204M"
    ]
  }
}


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


@mock.patch("public_data_report.hardware_report.hardware_report.build_device_map")
def test_transform_dimensions(mock_device_map):
    mock_device_map.return_value = DEVICE_MAP_SAMPLE

    test_data = {
        "browser_arch": [{"browser_arch": "x86-64", "client_count": 6}],
        "os": [
            {"os": "Windows_NT-10.0", "client_count": 1},
            {"os": "Windows_NT-6.2", "client_count": 5},
        ],
        "memory_gb": [
            {"memory_gb": 14, "client_count": 1},
            {"memory_gb": 17, "client_count": 5},
        ],
        "resolution": [
            {"resolution": "1920x1080", "client_count": 1},
            {"resolution": "2560x1440", "client_count": 5}
        ],
        "cpu_cores": [
            {"cpu_cores": 4, "client_count": 1},
            {"cpu_cores": 8, "client_count": 5},
        ],
        "cpu_vendor": [{"cpu_vendor": "GenuineIntel", "client_count": 6}],
        "cpu_speed": [
            {"cpu_speed": "3.6", "client_count": 1},
            {"cpu_speed": "Other", "client_count": 5},
        ],
        "has_flash": [
            {"has_flash": True, "client_count": 1},
            {"has_flash": False, "client_count": 5},
        ],
        "os_arch": [
            {
                "is_wow64": False,
                "os": "Windows_NT-6.2",
                "browser_arch": "x86-64",
                "client_count": 5,
            },
            {
                "is_wow64": True,
                "os": "Windows_NT-10.0",
                "browser_arch": "x86-64",
                "client_count": 1,
            },
        ],
        "gfx0_vendor_name": [
            {"gfx0_vendor_id": "0x10de", "client_count": 1},
            {"gfx0_vendor_id": "0x1414", "client_count": 5},
        ],
        "gfx0_model": [
            {"gfx0_device_id": "0x13c2", "gfx0_vendor_id": "0x10de", "client_count": 1},
            {"gfx0_device_id": "0xfefe", "gfx0_vendor_id": "0x1414", "client_count": 5},
        ],
    }

    transformed = hardware_report.transform_dimensions(test_data)

    transformed_expected = {
        "os": {"Windows_NT-10.0": 1, "Windows_NT-6.2": 5},
        "browser_arch": {"x86-64": 6},
        "cpu_cores": {4: 1, 8: 5},
        "cpu_vendor": {"GenuineIntel": 6},
        "cpu_speed": {"3.6": 1, "Other": 5},
        "resolution": {"1920x1080": 1, "2560x1440": 5},
        "memory_gb": {14: 1, 17: 5},
        "has_flash": {True: 1, False: 5},
        "os_arch": {"x86-64": 6},
        "gfx0_vendor_name": {"NVIDIA": 1, "Microsoft Basic": 5},
        "gfx0_model": {"Maxwell-GM204": 1, "Other": 5},
    }

    assert transformed == transformed_expected


def test_collapse_buckets():
    aggregated = {
        "os": {"Windows_NT-10.0": 95, "Windows_NT-6.2": 5},
        "arch": {"x86-64": 100},
        "cpu_cores": {4: 100},
        "cpu_vendor": {"GenuineIntel": 100},
        "cpu_speed": {"4": 2, "3.6": 48, "Other": 50},
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
        "cpu_speed": {"3.6": 0.48, "Other": 0.52},
        "resolution": {"1920x1080": 1.0},
        "memory_gb": {"14": 0.5, "17": 0.5},
        "has_flash": {"Other": 0.01, "False": 0.99},
        "os_arch": {"x86-64": 1.0},
        "gfx0_vendor_name": {"NVIDIA": 0.6, "Microsoft Basic": 0.4},
        "gfx0_model": {"Maxwell-GM204": 0.95, "Other": 0.05},
    }
    collapsed = hardware_report.collapse_buckets(aggregated, 10, 100)

    assert collapsed == collapsed_expected


@mock.patch("public_data_report.hardware_report.hardware_report.storage")
def test_upload_dryrun(mock_gcs):
    """"Dry run should not try to upload to GCS."""
    hardware_report.upload_data_gcs([], "", "", dryrun=True)
    assert mock_gcs.Client.call_count == 0

    hardware_report.upload_data_gcs([], "", "", dryrun=False)
    assert mock_gcs.Client.call_count == 1
