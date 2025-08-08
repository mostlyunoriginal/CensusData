import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CensusData
import os
from dotenv import load_dotenv

load_dotenv()

cd17 = CensusData(2017)

cd17.load_key(os.getenv("CENSUS_API_KEY"))

potential_products = cd17.list_products(pattern="acs 5")

for product in potential_products:
    print(product)

cd17.set_product("ACS 5-Year Detailed Tables")

potential_vars = cd17.list_variables(to_dicts=True, pattern="income")

for var in potential_vars:
    print(var)

vars_df = pl.DataFrame(potential_vars)
