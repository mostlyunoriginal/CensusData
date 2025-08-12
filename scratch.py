import sys

sys.modules.pop("CensusData", None)
from CensusData import CenDatHelper
import os
from dotenv import load_dotenv

load_dotenv()

cd = CenDatHelper(years=[2017], key=os.getenv("CENSUS_API_KEY"))

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

potential_geos = cd.list_geos(to_dicts=True)

for geo in potential_geos:
    print(geo["sumlev"], geo["desc"])

cd.set_geos("795")

potential_vars = cd.list_variables(
    to_dicts=True,
    patterns=["income", "person weight", "state", "public.*area"],
    logic=any,
)

for var in potential_vars:
    print(var["name"], var["label"])

cd.set_variables(["PUMA", "PWGTP", "HINCP", "ADJINC"])

response = cd.get_data(
    within=[
        {"state": "1", "public use microdata area": ["400", "2500"]},
        {"state": "4", "public use microdata area": "105"},
    ]
)

pums = response.to_polars()[0]
