"""A collection of tests for e-data utils"""

import json
import pathlib

import pytest

from ..processors import utils

TESTS_DIR = str(pathlib.Path(__file__).parent.resolve())
TEST_GOOD_INPUT = TESTS_DIR + "/inputs/utilities/edata.storage_TEST"
TEST_BAD_SUPPLY_INPUT = TEST_GOOD_INPUT + "_bad_supply"
TEST_BAD_CONTRACT_INPUT = TEST_GOOD_INPUT + "_bad_contract"
TEST_BAD_CONSUMPTION_INPUT = TEST_GOOD_INPUT + "_bad_consumption"
TEST_BAD_MAXPOWER_INPUT = TEST_GOOD_INPUT + "_bad_maxpower"


@pytest.mark.order(100)
def test_serialize():
    """Test import/export of serialized json data"""

    def check_file(filename):
        data = None
        with open(filename, "r", encoding="utf-8") as original_file:
            original_data = original_file.read()
            og_serialized_dict = json.loads(original_data)
            data = utils.deserialize_dict(og_serialized_dict)
            if data is None:
                return False
            serialized_dict = utils.serialize_dict(data)
            _comparable_fields = [
                "supplies",
                "contracts",
                "consumptions",
                "maximeter",
                "pvpc",
            ]
            if {
                x: serialized_dict[x]
                for x in serialized_dict
                if x in _comparable_fields
            } != {
                x: og_serialized_dict[x]
                for x in og_serialized_dict
                if x in _comparable_fields
            }:
                return False
        return True

    # test (de)serialization over good input
    assert check_file(TEST_GOOD_INPUT)

    # test (de)serialization over bad inputs
    for file in [
        TEST_BAD_SUPPLY_INPUT,
        TEST_BAD_CONTRACT_INPUT,
        TEST_BAD_CONSUMPTION_INPUT,
        TEST_BAD_MAXPOWER_INPUT,
    ]:
        assert not check_file(file)
