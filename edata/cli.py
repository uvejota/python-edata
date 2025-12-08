import asyncio
import logging
from getpass import getpass
from typing import Annotated

import typer

from edata.models.bill import BillingRules
from edata.providers.datadis import DatadisConnector
from edata.services.bill_service import BillService
from edata.services.data_service import DataService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def show_supplies(username: str):
    """Show supplies and contracts for a given datadis user."""

    password = getpass()
    connector = DatadisConnector(username, password)
    supplies = connector.get_supplies()
    typer.echo("\nSupplies:")
    for supply in supplies:
        typer.echo(supply.model_dump_json())
    if supplies:
        cups = supplies[0].cups
        distributor = supplies[0].distributorCode
        contracts = connector.get_contract_detail(cups, distributor)
        typer.echo("\nContracts:")
        for contract in contracts:
            typer.echo(contract.model_dump_json())
    else:
        typer.echo("We found no supplies.")


async def _download_all(
    nif: str,
    cups: str,
    authorized_nif: str | None = None,
):
    """Download all data for a given datadis account and CUPS."""

    password = getpass()
    service = DataService(
        cups,
        nif,
        password,
        datadis_authorized_nif=authorized_nif,
        storage_path="./edata_cli",
    )
    await service.update_supplies()
    supply = await service.get_supply()
    if supply:
        await service.update()


@app.command()
def download_all(
    nif: str,
    cups: Annotated[str, typer.Option(help="The identifier of the Supply")],
    authorized_nif: str | None = None,
):
    """Download all data for a given datadis account and CUPS."""

    asyncio.run(_download_all(nif, cups, authorized_nif))


async def _update_custom_bill(
    cups: str,
    p1_kw_year_eur: float,
    p2_kw_year_eur: float,
    p1_kwh_eur: float,
    p2_kwh_eur: float,
    p3_kwh_eur: float,
    meter_month_eur: float,
):
    """Download all data for a given datadis account and CUPS."""

    bs = BillService(cups, storage_path="./edata_cli")
    rules = BillingRules(
        p1_kw_year_eur=p1_kw_year_eur,
        p2_kw_year_eur=p2_kw_year_eur,
        p1_kwh_eur=p1_kwh_eur,
        p2_kwh_eur=p2_kwh_eur,
        p3_kwh_eur=p3_kwh_eur,
        meter_month_eur=meter_month_eur,
    )
    await bs.update(billing_rules=rules, is_pvpc=False)


@app.command()
def update_custom_bill(
    cups: Annotated[str, typer.Option(help="The identifier of the Supply")],
    p1_kw_year_eur: Annotated[
        float, typer.Option(help="Price per kW at P1 tariff (by year)")
    ],
    p2_kw_year_eur: Annotated[
        float, typer.Option(help="Price per kW at P2 tariff (by year)")
    ],
    p1_kwh_eur: Annotated[float, typer.Option(help="Price per kWh at P1 tariff")],
    p2_kwh_eur: Annotated[float, typer.Option(help="Price per kWh at P2 tariff")],
    p3_kwh_eur: Annotated[float, typer.Option(help="Price per kWh at P3 tariff")],
    meter_month_eur: Annotated[float, typer.Option(help="Monthly cost of the meter")],
):
    """Download all data for a given datadis account and CUPS."""

    asyncio.run(
        _update_custom_bill(
            cups,
            p1_kw_year_eur,
            p2_kw_year_eur,
            p1_kwh_eur,
            p2_kwh_eur,
            p3_kwh_eur,
            meter_month_eur,
        )
    )


async def _update_pvpc_bill(cups: str):
    """Download all data for a given datadis account and CUPS."""

    bs = BillService(cups, storage_path="./edata_cli")
    await bs.update(is_pvpc=True)


@app.command()
def update_pvpc_bill(
    cups: Annotated[str, typer.Option(help="The identifier of the Supply")],
):
    """Download all data for a given datadis account and CUPS."""

    asyncio.run(
        _update_pvpc_bill(
            cups,
        )
    )


if __name__ == "__main__":
    app()
