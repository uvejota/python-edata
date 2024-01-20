"""A collection of tests for e-data processors"""

import datetime as dt
import json
import pathlib
import typing
from collections.abc import Iterable

import pytest

from ..definitions import PricingData, PricingRules
from ..processors import utils
from ..processors.base import Processor
from ..processors.billing import BillingProcessor
from ..processors.consumption import ConsumptionProcessor
from ..processors.maximeter import MaximeterProcessor

TESTS_DIR = str(pathlib.Path(__file__).parent.resolve())
TEST_GOOD_INPUT = TESTS_DIR + "/assets/processors/edata.storage_TEST"
TEST_EXPECTATIONS = TESTS_DIR + "/assets/processors/{key}.out"


def _compare_processor_output(
    source_filepath: str,
    expectations_filepath: str,
    processor_class: Processor,
    key: str,
):
    with open(source_filepath, encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))
        if key == "consumptions":
            processor = processor_class({"consumptions": data[key]})
        else:
            processor = processor_class(data[key])
        # with open(expectations_filepath, "w", encoding="utf-8") as expectations_file:
        #     json.dump(utils.serialize_dict(processor.output), expectations_file)
        with open(expectations_filepath, encoding="utf-8") as expectations_file:
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


@pytest.mark.order(1001)
@pytest.mark.parametrize(
    "_id, rules, prices",
    [
        (
            "custom_prices",
            PricingRules(
                p1_kw_year_eur=30.67266,
                p2_kw_year_eur=1.4243591,
                meter_month_eur=0.81,
                market_kw_year_eur=3.113,
                electricity_tax=1.0511300560,
                iva_tax=1.1,
                p1_kwh_eur=None,
                p2_kwh_eur=None,
                p3_kwh_eur=None,
            ),
            [
                PricingData(
                    datetime=dt.datetime(2022, 10, 22, x, 0, 0),
                    value_eur_kWh=1,
                    delta_h=1,
                )
                for x in range(0, 24)
            ],
        ),
        (
            "constant_prices",
            PricingRules(
                p1_kw_year_eur=30.67266,
                p2_kw_year_eur=1.4243591,
                meter_month_eur=0.81,
                market_kw_year_eur=3.113,
                electricity_tax=1.0511300560,
                iva_tax=1.1,
                p1_kwh_eur=1,
                p2_kwh_eur=1,
                p3_kwh_eur=1,
            ),
            None,
        ),
    ],
)
def test_processor_billing(
    _id: str, rules: PricingRules, prices: typing.Optional[Iterable[PricingData]]
):
    """Tests billing processor"""
    with open(TEST_GOOD_INPUT, "r", encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))
        processor = BillingProcessor(
            {
                "consumptions": data["consumptions"],
                "contracts": data["contracts"],
                "prices": prices,
                "rules": rules,
            }
        )
    # with open(TEST_EXPECTATIONS.format(key=f"billing-{_id}"), "w", encoding="utf-8") as expectations_file:
    #     json.dump(utils.serialize_dict(processor.output), expectations_file)
    with open(
        TEST_EXPECTATIONS.format(key=f"billing-{_id}"), "r", encoding="utf-8"
    ) as expectations_file:
        expected_output = json.load(expectations_file)
        assert utils.serialize_dict(processor.output) == expected_output
