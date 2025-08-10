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

for product in cd.list_products(
    patterns=[
        "american community|acs",
        "public use micro|pums",
        "5-year",
        "^(?!.*puerto rico).*$",
    ]
):
    print(product["title"], product["vintage"])

cd.set_products()

potential_geos = cd.list_geos(to_dicts=True, patterns="^state$")

for geo in potential_geos:
    print(geo["sumlev"], geo["desc"])

cd.set_geos()

potential_vars = cd.list_variables(
    to_dicts=True,
    patterns=["income", "person weight"],
    logic=any,
)

for var in potential_vars:
    print(var["name"], var["label"])

cd.set_variables(["PWGTP", "HINCP", "ADJINC"])
