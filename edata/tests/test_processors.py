"""A collection of tests for e-data processors"""

import json
import pathlib
import datetime as dt

import pytest

from ..processors import utils
from ..processors.base import Processor
from ..processors.consumption import ConsumptionProcessor
from ..processors.maximeter import MaximeterProcessor
from ..processors.billing import BillingProcessor
from ..definitions import PricingData, PricingRules

TESTS_DIR = str(pathlib.Path(__file__).parent.resolve())
TEST_GOOD_INPUT = TESTS_DIR + "/inputs/processors/edata.storage_TEST"
TEST_EXPECTATIONS = TESTS_DIR + "/inputs/processors/{key}.out"


def _compare_processor_output(
    source_filepath: str,
    expectations_filepath: str,
    processor_class: Processor,
    key: str,
):
    with open(source_filepath, "r", encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))
        processor = processor_class(data[key])
        with open(expectations_filepath, "r", encoding="utf-8") as expectations_file:
            expected_output = json.load(expectations_file)
            assert utils.serialize_dict(processor.output) == expected_output


@pytest.mark.order(1000)
@pytest.mark.parametrize(
    "processor, key",
    [(ConsumptionProcessor, "consumptions"), (MaximeterProcessor, "maximeter")],
)
def test_processor(processor: Processor, key: str) -> None:
    """Tests all processors but billing"""
    _compare_processor_output(
        TEST_GOOD_INPUT,
        TEST_EXPECTATIONS.format(key=key),
        processor,
        key,
    )


def test_processor_billing():
    """Tests billing processor"""
    rules = PricingRules(
        p1_kw_year_eur=30.67266,
        p2_kw_year_eur=1.4243591,
        meter_month_eur=0.81,
        market_kw_year_eur=3.113,
        electricity_tax=1.0511300560,
        iva_tax=1.1,
    )
    with open(TEST_GOOD_INPUT, "r", encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))
        processor = BillingProcessor(
            {
                "consumptions": data["consumptions"],
                "contracts": data["contracts"],
                "prices": [
                    PricingData(
                        datetime=dt.datetime(2022, 10, 22, x, 0, 0),
                        value_eur_kWh=1,
                        delta_h=1,
                    )
                    for x in range(0, 24)
                ],
                "rules": rules,
            }
        )

    with open(
        TEST_EXPECTATIONS.format(key="billing"), "r", encoding="utf-8"
    ) as expectations_file:
        expected_output = json.load(expectations_file)
        assert utils.serialize_dict(processor.output) == expected_output
