from public_data_report.hardware_report import hardware_report


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
        == "Unknown"
    ), "Unknown devices must be reported as 'Unknown'."
    assert (
        hardware_report.get_device_family_chipset("0xfeeb", "0xdeee", device_map)
        == "Unknown"
    ), "Unknown families must be reported as 'Unknown'."
