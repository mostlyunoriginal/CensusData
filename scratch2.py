import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CenDatHelper
import os
from dotenv import load_dotenv

load_dotenv()

cdh = CenDatHelper(years=[2022, 2023], key=os.getenv("CENSUS_API_KEY"))

potential_products = cdh.list_products(
    to_dicts=True,
    patterns=[
        "american community|acs",
        "5-year",
        "detailed",
        "^(?!.*(alaska|aian|selected)).*$",
    ],
)

for product in potential_products:
    print(product["title"], product["vintage"])

cdh.set_products()

for geo in cdh.list_geos(to_dicts=True):
    print(geo)

cdh.set_geos(["155"])

potential_variables = cdh.list_variables(
    to_dicts=True, patterns=["total", "less.*high"]
)

for variable in potential_variables:
    print(variable["name"], variable["label"])

cdh.set_variables(["B07009_002E", "B16010_009E"])

response = cdh.get_data(
    max_workers=200,
    within=[
        {"state": "36", "place": ["61797", "61621"]},
        {"state": "06"},
    ],
)

test = pl.concat(response.to_polars())
