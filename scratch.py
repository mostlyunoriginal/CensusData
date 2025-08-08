import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd17 = CensusData(2017)

cd17.load_key(os.getenv("CENSUS_API_KEY"))

potential_products = cd17.list_products(to_dicts=False, pattern="acs 5")

for product in potential_products:
    print(product)

cd17.set_product("ACS 5-Year Detailed Tables")

potential_geos = cd17.list_geos(to_dicts=True)

for geo in potential_geos:
    print(geo)

potential_vars = cd17.list_variables(
    to_dicts=True,
    pattern=["median household income", "inflation-adjusted", "total"],
)

for var in potential_vars:
    print(var["label"])

vars_df = pl.DataFrame(potential_vars)

print(vars_df)
