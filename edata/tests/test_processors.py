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


def _compare_processor_output(
    source_filepath: str,
    processor_class: Processor,
    key: str,
    snapshot,
):
    with open(source_filepath, encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))
        if key == "consumptions":
            processor = processor_class({"consumptions": data[key]})
        else:
            processor = processor_class(data[key])
        assert utils.serialize_dict(processor.output) == snapshot


@pytest.mark.parametrize(
    "processor, key",
    [(ConsumptionProcessor, "consumptions"), (MaximeterProcessor, "maximeter")],
)
def test_processor(processor: Processor, key: str, snapshot) -> None:
    """Tests all processors but billing (syrupy snapshot)"""
    _compare_processor_output(
        TEST_GOOD_INPUT,
        processor,
        key,
        snapshot,
    )


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
    _id: str,
    rules: PricingRules,
    prices: typing.Optional[Iterable[PricingData]],
    snapshot,
):
    """Tests billing processor (syrupy snapshot)"""
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
    assert utils.serialize_dict(processor.output) == snapshot
