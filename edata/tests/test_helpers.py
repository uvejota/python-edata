"""A collection of tests for e-data processors"""

import json
import pathlib

import pytest
from freezegun import freeze_time

from ..definitions import PricingRules
from ..helpers import EdataHelper
from ..processors import utils

AT_TIME = "2022-10-22"
TESTS_DIR = str(pathlib.Path(__file__).parent.resolve())
TEST_GOOD_INPUT = TESTS_DIR + "/assets/helpers/edata.storage_TEST"
TEST_EXPECTATIONS_DATA = TESTS_DIR + "/assets/helpers/data.out"
TEST_EXPECTATIONS_ATTRIBUTES = (
    TESTS_DIR + f"/assets/helpers/attributes_at_{AT_TIME}.out"
)

PRICING_RULES_PVPC = PricingRules(
    p1_kw_year_eur=30.67266,
    p2_kw_year_eur=1.4243591,
    meter_month_eur=0.81,
    market_kw_year_eur=3.113,
    electricity_tax=1.0511300560,
    iva_tax=1.05,
    p1_kwh_eur=None,
    p2_kwh_eur=None,
    p3_kwh_eur=None,
)


@pytest.mark.order(10000)
@freeze_time(AT_TIME)
def test_helper_offline() -> None:
    """Tests EdataHelper"""

    with open(TEST_GOOD_INPUT, "r", encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))

        helper = EdataHelper(
            "USER",
            "PASS",
            "CUPS",
            datadis_authorized_nif=None,
            pricing_rules=PRICING_RULES_PVPC,
            data=data,
        )
        helper.process_data()

        with open(TEST_EXPECTATIONS_DATA, "r", encoding="utf-8") as expectations_file:
            assert utils.serialize_dict(helper.data) == json.load(expectations_file)

        with open(
            TEST_EXPECTATIONS_ATTRIBUTES, "r", encoding="utf-8"
        ) as expectations_file:
            assert utils.serialize_dict(helper.attributes) == json.load(
                expectations_file
            )

    assert True
