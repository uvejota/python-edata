import asyncio
import logging

from .connectors import *
from .processors import *

_LOGGER = logging.getLogger(__name__)


class EdataHelper:
    def __init__(self, username, password, cups) -> None:

        # prepare some parameters
        self._cups = cups
        self.data = EdataData(
            supplies=[], contracts=[], consumptions={}, maximeter={}, costs={}
        )
        self.last_updated = {}
        self.last_changed = {}

        # create api objects
        self._datadis = DatadisConnector(username, password)
        self._esios = EsiosConnector()

        # define processing stages
        self._stages = [
            self.process_consumptions,
            self.process_maximeter,
            self.process_pvpc,
        ]

    def update(self, date_from=None, date_to=None):
        if self.fetch_data(date_from, date_to):
            self.process()

    def fetch_data(self, date_from=None, date_to=None):
        try:
            if self._datadis.update(self._cups, date_from, date_to):
                self.data["supplies"] = self._datadis.data["supplies"]
                self.data["contracts"] = self._datadis.data["contracts"]
                self.data["consumptions"]["raw"] = self._datadis.data["consumptions"]
                self.data["maximeter"]["raw"] = self._datadis.data["maximeter"]
                self.last_updated["datadis"] = self._datadis.last_updated
                if len(self.data["consumptions"]["raw"]) > 0:
                    self._esios.update(
                        self.data["consumptions"]["raw"][0]["start"],
                        self.data["consumptions"]["raw"][-1]["end"],
                    )
                    self.data["costs"]["pvpc"] = self._esios.data["energy_costs"]
                    self.last_updated["esios"] = self._esios.last_updated
                return True
        except (ServerError, TimeoutError) as e:
            _LOGGER.exception("unhandled exception during data fetch %s", e)
        return False

    def process(self):
        for f in self._stages:
            try:
                f()
            except Exception as e:
                _LOGGER.exception("unhandled exception during stage %s", e)

    def process_consumptions(self):
        if "raw" in self.data["consumptions"]:
            proc = ConsumptionProcessor(self.data["consumptions"]["raw"])
            self.data["consumptions"]["hourly"] = proc.output["hourly"]
            self.data["consumptions"]["daily"] = proc.output["daily"]
            self.data["consumptions"]["monthly"] = proc.output["monthly"]

    def process_maximeter(self):
        if "raw" in self.data["maximeter"]:
            processor = MaximeterProcessor(self.data["maximeter"]["raw"])
            self.data["maximeter"]["hourly"] = processor.output["hourly"]
            self.data["maximeter"]["stats"] = processor.output["stats"]

    def process_pvpc(self):
        if "pvpc" in self.data["costs"]:
            _input = {
                "consumptions": self.data["consumptions"]["raw"],
                "contracts": self.data["contracts"],
                "energy_costs": self.data["costs"]["pvpc"],
            }
            processor = BillingProcessor(_input)
            self.data["costs"]["hourly"] = processor.output["hourly"]
            self.data["costs"]["daily"] = processor.output["daily"]
            self.data["costs"]["monthly"] = processor.output["monthly"]
