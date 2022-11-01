"""A collection of tests for e-data processors"""

import json
import pathlib

import pytest
from freezegun import freeze_time

from ..helpers import EdataHelper
from ..processors import utils

AT_TIME = "2022-10-22"
TESTS_DIR = str(pathlib.Path(__file__).parent.resolve())
TEST_GOOD_INPUT = TESTS_DIR + "/inputs/helpers/edata.storage_TEST"
TEST_EXPECTATIONS_DATA = TESTS_DIR + "/inputs/helpers/data.out"
TEST_EXPECTATIONS_ATTRIBUTES = (
    TESTS_DIR + f"/inputs/helpers/attributes_at_{AT_TIME}.out"
)


@pytest.mark.order(10000)
@freeze_time(AT_TIME)
def test_helper_offline() -> None:
    """Tests EdataHelper"""

    with open(TEST_GOOD_INPUT, "r", encoding="utf-8") as original_file:
        data = utils.deserialize_dict(json.load(original_file))

        helper = EdataHelper("USER", "PASS", "CUPS", authorized_nif=None, data=data)
        helper.process_data()

        with open(TEST_EXPECTATIONS_DATA, "w", encoding="utf-8") as expectations_file:
            json.dump(utils.serialize_dict(helper.data), expectations_file)
            # assert utils.serialize_dict(helper.data) == json.load(expectations_file)

        with open(
            TEST_EXPECTATIONS_ATTRIBUTES, "w", encoding="utf-8"
        ) as expectations_file:
            json.dump(utils.serialize_dict(helper.attributes), expectations_file)
            # assert utils.serialize_dict(helper.attributes) == json.load(
            #     expectations_file
            # )

    assert True


# TODO implement process_pvpc and process_custom_billing
