import pytest
from freezegun import freeze_time

from edata.helpers import EdataHelper
from edata.models.supply import Supply, Contract
from edata.models.data import Energy, Power
from edata.services.data_service import DataService

import os
import json
import pytest

ASSETS = os.path.join(os.path.dirname(__file__), "assets", "data")


def load_json(filename):
    with open(os.path.join(ASSETS, filename), encoding="utf-8") as f:
        return json.load(f)


def load_models(filename, model):
    return [model(**d) for d in load_json(filename)]


@pytest.fixture
def supplies():
    return load_models("supplies.json", Supply)


@pytest.fixture
def contracts():
    return load_models("contracts.json", Contract)


@pytest.fixture
def energy():
    return load_models("energy.json", Energy)


@pytest.fixture
def power():
    return load_models("power.json", Power)


def test_edata_helper_update(supplies, contracts, energy, power, snapshot):

    # Instanciar DataService real y sobrescribir los datos
    real_service = DataService(
        cups="ESXXXXXXXXXXXXXXXXTEST",
        datadis_user="user",
        datadis_pwd="pass",
        datadis_authorized_nif=None,
        storage_path=None,
    )
    real_service._supplies = supplies
    real_service._contracts = contracts
    real_service._energy = energy
    real_service._power = power

    # Inicializar EdataHelper con los parámetros obligatorios
    helper = EdataHelper(
        datadis_username="user",
        datadis_password="pass",
        cups="ESXXXXXXXXXXXXXXXXTEST",
        datadis_authorized_nif=None,
        pricing_rules=None,
        storage_dir_path=None,
        data=None,
    )
    helper.data_service = real_service

    helper.update()

    assert helper.data == snapshot


@freeze_time("2022-10-22")
def test_edata_helper_attributes(supplies, contracts, energy, power, snapshot):
    real_service = DataService(
        cups="ESXXXXXXXXXXXXXXXXTEST",
        datadis_user="user",
        datadis_pwd="pass",
        datadis_authorized_nif=None,
        storage_path=None,
    )
    real_service._supplies = supplies
    real_service._contracts = contracts
    real_service._energy = energy
    real_service._power = power

    helper = EdataHelper(
        datadis_username="user",
        datadis_password="pass",
        cups="ESXXXXXXXXXXXXXXXXTEST",
        datadis_authorized_nif=None,
        pricing_rules=None,
        storage_dir_path=None,
        data=None,
    )
    helper.data_service = real_service
    helper.update()
    helper.process_data()
    assert helper.attributes == snapshot
