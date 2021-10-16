from typing import Final
from .data import CostsData, FixedCostsData, TariffData
from datetime import datetime

anonymize: Final = lambda string : "_" + string[-int(len(string)/2):] if isinstance (string, str) else string

ERROR_SERVER_FAILURE: Final = lambda user, query, response: f"server error on {anonymize(user)}: {query}:{response}"
ERROR_SERVER_EMPTY: Final = lambda user, query, response: f"server empty response on {anonymize(user)}: {query}:{response}"
ERROR_TIMEOUT: Final = lambda seconds, user, query: f"server timeout error ({seconds}s) on {anonymize(user)}: {query}"
ERROR_LOGIN_AUTH: Final = lambda user: f"wrong credentials for {anonymize(user)}"
ERROR_CUPS: Final = lambda cups, cups_list: f"cups {anonymize(cups)} not found at server, available are: {cups_list}"


RULES_PVPC: Final = [
    CostsData (
        start = datetime (2021, 6, 1),
        end = datetime (2021, 9, 14),
        tariffs={
            'p1': TariffData (
                hours= [10, 11, 12, 13, 18, 19, 20, 21],
                weekdays=[],
                cost_kwh=None
                ),
            'p2': TariffData (
                hours= [8, 9, 14, 15, 16, 17, 22, 23],
                weekdays=[],
                cost_kwh=None
                ),
            'p3': TariffData (
                hours= [0, 1, 2, 3, 4, 5, 6, 7],
                weekdays=[5, 6],
                cost_kwh=None
                ),
        },
        fixed_costs_daily = FixedCostsData (
            cost_p1_kw = 30.67266 / 365,
            cost_p2_kw = 1.4243591 / 365,
            market_p1_kw = 3.113 / 365,
            others_day = 0.81 / 30,
            electricity_tax = 1.0511300560,
            iva_tax = 1.1,
        )
    ),
    CostsData (
        start=datetime (2021, 9, 14),
        end=datetime (2022, 1, 1),
        tariffs={
            'p1': TariffData (
                hours= [10, 11, 12, 13, 18, 19, 20, 21],
                weekdays=[],
                cost_kwh=None
                ),
            'p2': TariffData (
                hours= [8, 9, 14, 15, 16, 17, 22, 23],
                weekdays=[],
                cost_kwh=None
                ),
            'p3': TariffData (
                hours= [0, 1, 2, 3, 4, 5, 6, 7],
                weekdays=[5, 6],
                cost_kwh=None
                ),
        },
        fixed_costs_daily = FixedCostsData (
            cost_p1_kw = 30.67266 / 365,
            cost_p2_kw = 1.4243591 / 365,
            market_p1_kw = 3.113 / 365,
            others_day = 0.81 / 30,
            electricity_tax = 1.005,
            iva_tax = 1.1,
        )
    ),
    CostsData (
        start=datetime (2022, 1, 1),
        end=None,
        tariffs={
            'p1': TariffData (
                hours= [10, 11, 12, 13, 18, 19, 20, 21],
                weekdays=[],
                cost_kwh=None
                ),
            'p2': TariffData (
                hours= [8, 9, 14, 15, 16, 17, 22, 23],
                weekdays=[],
                cost_kwh=None
                ),
            'p3': TariffData (
                hours= [0, 1, 2, 3, 4, 5, 6, 7],
                weekdays=[5, 6],
                cost_kwh=None
                ),
        },
        fixed_costs_daily = FixedCostsData (
            cost_p1_kw = 30.67266 / 365,
            cost_p2_kw = 1.4243591 / 365,
            market_p1_kw = 3.113 / 365,
            others_day = 0.81 / 30,
            electricity_tax = 1.0511300560,
            iva_tax = 1.21,
        )
    )
]