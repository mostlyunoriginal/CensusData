import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd = CensusData(years=[2017], key=os.getenv("CENSUS_API_KEY"))

potential_products = cd.list_products(
    patterns=[
        "american community|acs",
        "public use micro|pums",
        "5-year",
        "^(?!.*puerto rico).*$",
    ]
)

for product in potential_products:
    print(product["title"], product["vintage"])

cd.set_products()

potential_geos = cd.list_geos(to_dicts=True, patterns="public")

for geo in potential_geos:
    print(geo["sumlev"], geo["desc"])

cd.set_geos()

potential_vars = cd.list_variables(
    to_dicts=True,
    patterns=["income", "person weight", "state", "public.*area"],
    logic=any,
)

for var in potential_vars:
    print(var["name"], var["label"])

cd.set_variables(["ST", "PUMA", "PWGTP", "HINCP", "ADJINC"])

data = cd.get_data(within={"state": ["06"]})

df_06 = pl.DataFrame(
    data[1:],
    schema=data[0],
    orient="row",
    schema_overrides={"HINCP": pl.Int64, "PWGTP": pl.Float64, "ADJINC": pl.Float64},
)

print(df_06)
