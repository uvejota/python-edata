import typer
from getpass import getpass
from edata.connectors.datadis import DatadisConnector
import json

def main():
    """CLI básica para mostrar supplies y contracts de Datadis."""
    username = typer.prompt("Usuario (NIF)")
    password = getpass("Contraseña: ")
    connector = DatadisConnector(username, password)
    supplies = connector.get_supplies()
    typer.echo("\nSupplies:")
    typer.echo(json.dumps(supplies, indent=2, default=str))
    if supplies:
        cups = supplies[0]["cups"]
        distributor = supplies[0]["distributorCode"]
        contracts = connector.get_contract_detail(cups, distributor)
        typer.echo("\nContracts:")
        typer.echo(json.dumps(contracts, indent=2, default=str))
    else:
        typer.echo("No se encontraron supplies.")

if __name__ == "__main__":
    typer.run(main)
