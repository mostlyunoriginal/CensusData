import sys
import polars as pl

sys.modules.pop("CensusData", None)
from CensusData import CenDatHelper
import os
from dotenv import load_dotenv

load_dotenv()

cdh = CenDatHelper(key=os.getenv("CENSUS_API_KEY"))

potential_products = cdh.list_products(patterns=["cps"])

for product in potential_products:
    print(product["title"], product["vintage"], product["is_microdata"], product["url"])

cdh.set_years(2019)

potential_products = cdh.list_products(patterns=["cps basic"])

for product in potential_products:
    print(product["title"], product["vintage"], product["is_microdata"], product["url"])

cdh.set_products()

potential_variables = cdh.list_variables(
    patterns=[
        # "unemployed","school","weight","age",
        "month"
    ],
    logic=any,
)

for variable in potential_variables:
    print(variable["name"], variable["label"], variable["vintage"])

cdh.set_variables(["PELKAVL", "PEEDUCA", "PRTAGE", "PWCMPWGT", "PWLGWGT"])

cdh.list_geos(to_dicts=True)

cdh.set_geos("state", by="desc")

response = cdh.get_data(within={"state": "08"})

cdh._create_params()
cdh._explode_params()

response = cdh.get_data(within={"state": "08"})

dfs = pl.concat(
    response.to_polars(
        schema_overrides={
            "PELKAVL": pl.Int64,
            "PEEDUCA": pl.Int64,
            "PRTAGE": pl.Int64,
            "PWCMPWGT": pl.Float64,
            "PWLGWGT": pl.Float64,
        }
    )
)
