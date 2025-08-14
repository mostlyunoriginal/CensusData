import sys

sys.modules.pop("CensusData", None)
from CensusData import CenDatHelper
import os
from dotenv import load_dotenv

load_dotenv()

cdh = CenDatHelper(key=os.getenv("CENSUS_API_KEY"))

potential_products = cdh.list_products(patterns=[r"acs/acs5\)"], years=[2022, 2023])

cdh.set_products()
cdh.set_variables(names=["B25010_001E", "B19013_001E"])
cdh.set_geos("150")

response = cdh.get_data(
    max_workers=50, within=[{"state": "08", "county": "069"}, {"state": "56"}]
)
