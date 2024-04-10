import json
from unittest import mock

from public_data_report import USER_ACITVITY_COUNTRY_LIST
from public_data_report.annotations.annotations import get_usage_annotations


@mock.patch(
    "public_data_report.annotations.annotations.DEFAULT_USAGE_ANNOTATIONS",
    [{"annotation": "abc"}],
)
@mock.patch(
    "public_data_report.annotations.annotations.USER_ACITVITY_COUNTRY_LIST",
    ["Brazil", "Canada", "France"],
)
@mock.patch("public_data_report.annotations.annotations.json")
def test_default_annotations(mock_json):
    """Default annotations should be appended to each country in annotations_webusage.json."""
    mock_json.loads.return_value = {"Brazil": [{"annotation": "123"}]}
    mock_json.dumps = json.dumps

    actual = get_usage_annotations()

    expected = json.dumps(
        {
            "Brazil": [
                {"annotation": "123"},
                {"annotation": "abc"},
            ],
            "Canada": [
                {"annotation": "abc"},
            ],
            "France": [
                {"annotation": "abc"},
            ],
        },
        indent=2,
        sort_keys=True,
    )

    assert actual == expected


def test_usage_annotations_countries():
    """Countries in annotations_webusage should exactly match the USER_ACITVITY_COUNTRY_LIST."""
    actual = json.loads(get_usage_annotations()).keys()

    assert len(actual) == len(USER_ACITVITY_COUNTRY_LIST)
    assert set(actual) == set(USER_ACITVITY_COUNTRY_LIST)
