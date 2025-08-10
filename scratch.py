import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd = CensusData(key=os.getenv("CENSUS_API_KEY"))

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

cd.set_products()

potential_geos = cd.list_geos(to_dicts=True, patterns="^place$")

for geo in potential_geos:
    print(geo["sumlev"], geo["desc"])

cd.set_geos()

potential_vars = cd.list_variables(
    to_dicts=True,
    patterns=["median household income", "inflation-adjusted", "total"],
)

for var in potential_vars:
    print(var["name"], var["label"])

cd.set_variables("B19049_001E")
