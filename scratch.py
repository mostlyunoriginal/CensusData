import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd = CensusData()

cd.load_key(os.getenv("CENSUS_API_KEY"))

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

products_df = pl.DataFrame(potential_products)

cd.set_products(
    [
        "ACS 5-Year Detailed Tables",
        "American Community Survey: 5-Year Estimates: Detailed Tables 5-Year",
    ]
)

potential_geos = cd.list_geos(to_dicts=True)

for geo in potential_geos:
    print(geo)

geos_df = pl.DataFrame(potential_geos)

potential_vars = cd.list_variables(
    to_dicts=True,
    patterns=["median household income", "inflation-adjusted", "total"],
)

for var in potential_vars:
    print(var["name"], var["applies_to"])

vars_df = pl.DataFrame(potential_vars)

print(vars_df)
