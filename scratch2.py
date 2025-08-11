import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd = CensusData(years=[2022, 2023], key=os.getenv("CENSUS_API_KEY"))

potential_products = cd.list_products(
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

cd.set_products()

for geo in cd.list_geos(to_dicts=True):
    print(geo)

cd.set_geos(["155", "160"])

potential_variables = cd.list_variables(to_dicts=True, patterns=["total", "less.*high"])

for variable in potential_variables:
    print(variable["name"], variable["label"])

cd.set_variables(["B07009_002E", "B16010_009E"])

cd._create_params()
cd._explode_params()
