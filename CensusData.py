import os
import re
import requests
from typing import List, Union, Dict, Optional, Callable


class CensusData:
    """
    A helper object for exploring and working with the US Census Bureau API.

    This class provides methods to list and select datasets, geographies, and
    variables by interacting directly with the Census JSON API endpoints.

    Attributes:
        years (list[int]): The primary year or years of interest for data queries.
        product (dict): The currently selected data product details, including
                        title, description, vintage, type, and URL.
    """

    def __init__(self, years: Optional[Union[int, List[int]]] = None):
        """
        Initializes the CensusData object.

        Args:
            years (int | list[int], optional): The year or years of interest.
                                               If provided, they are set upon
                                               initialization. Defaults to None.
        """
        self.years: Optional[List[int]] = None
        self.product: Optional[Dict[str, str]] = None
        self.__key: Optional[str] = None
        self._products_cache: Optional[List[Dict[str, str]]] = None

        if years is not None:
            self._set_years(years)

    def _set_years(self, years: Union[int, List[int]]):
        """Internal method to set the object's years attribute."""
        if isinstance(years, int):
            self.years = [years]
        elif isinstance(years, list) and all(isinstance(y, int) for y in years):
            self.years = sorted(list(set(years)))
        else:
            raise TypeError("'years' must be an integer or a list of integers.")
        print(f"✅ Years set to: {self.years}")

    def load_key(self, key: Optional[str] = None):
        """Loads a Census API key for authenticated requests."""
        if key:
            self.__key = key
            print("✅ API key loaded successfully.")
        else:
            print("⚠️ No API key provided. API requests may have stricter rate limits.")

    def _get_json_from_url(self, url: str) -> Optional[Dict]:
        """Helper to fetch and parse JSON from a URL."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data from {url}: {e}")
        except requests.exceptions.JSONDecodeError:
            print(f"❌ Failed to decode JSON from {url}")
        return None

    def _parse_vintage(self, vintage_input: Union[str, int]) -> List[int]:
        """
        Robustly parses a vintage value which could be an int, a string of an
        int, or a string representing a range (e.g., '2017-2021').
        """
        if not vintage_input:
            return []

        vintage_str = str(vintage_input)
        try:
            if "-" in vintage_str:
                start, end = map(int, vintage_str.split("-"))
                return list(range(start, end + 1))
            return [int(vintage_str)]
        except (ValueError, TypeError):
            return []

    def list_products(
        self,
        years: Optional[Union[int, List[int]]] = None,
        pattern: Optional[Union[str, List[str]]] = None,
        to_dicts: bool = True,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available data products from the JSON endpoint.

        Args:
            years (int | list[int], optional): Filters products for these years.
            pattern (str | list[str], optional): A regex pattern or list of
                patterns to filter products by title.
            to_dicts (bool): If True (default), returns a list of dictionaries.
                             If False, returns a list of product titles.
            logic (callable): `all` (default) or `any`. Determines if all
                              patterns must match or if any can match.
        """
        if not self._products_cache:
            data = self._get_json_from_url("https://api.census.gov/data.json")
            if not data or "dataset" not in data:
                return []

            products = []
            for d in data["dataset"]:
                access_url = next(
                    (
                        dist.get("accessURL")
                        for dist in d.get("distribution", [])
                        if "api.census.gov/data" in dist.get("accessURL", "")
                    ),
                    None,
                )
                if not access_url:
                    continue

                c_dataset_val = d.get("c_dataset")
                dataset_type = "N/A"
                if isinstance(c_dataset_val, list) and len(c_dataset_val) > 1:
                    dataset_type = c_dataset_val[1]

                products.append(
                    {
                        "title": d.get("title"),
                        "desc": d.get("description"),
                        "vintage": self._parse_vintage(d.get("c_vintage")),
                        "type": dataset_type,
                        "url": access_url,
                    }
                )
            self._products_cache = products

        if years is not None:
            target_years = [years] if isinstance(years, int) else list(years)
        else:
            target_years = self.years

        filtered = self._products_cache
        if target_years:
            target_set = set(target_years)
            filtered = [
                p
                for p in filtered
                if p.get("vintage") and target_set.intersection(p["vintage"])
            ]

        if pattern:
            patterns = [pattern] if isinstance(pattern, str) else pattern
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in patterns]
                filtered = [
                    p
                    for p in filtered
                    if p.get("title")
                    and logic(regex.search(p["title"]) for regex in regexes)
                ]
            except re.error as e:
                print(f"❌ Invalid regex pattern: {e}")
                return []

        return filtered if to_dicts else [p["title"] for p in filtered]

    def set_product(self, title: str):
        """Sets the active data product for the object."""
        all_products = self.list_products(to_dicts=True, years=[])
        if not all_products:
            print("❌ Error: Could not retrieve product list.")
            return

        matching_products = [p for p in all_products if p.get("title") == title]

        if not matching_products:
            print(f"❌ Error: No product with the title '{title}' was found.")
            return

        found_product = None
        if self.years:
            target_set = set(self.years)
            found_product = next(
                (
                    p
                    for p in matching_products
                    if target_set.intersection(p.get("vintage", []))
                ),
                None,
            )
            if not found_product:
                print(
                    f"❌ Error: Product '{title}' is not available for the specified years: {self.years}."
                )
                return
        else:
            if len(matching_products) > 1:
                vintages = sorted(
                    [v for p in matching_products for v in p.get("vintage", [])]
                )
                print(
                    f"❌ Error: The product title '{title}' is ambiguous and exists for multiple vintages: {vintages}."
                )
                print(
                    "ℹ️ Please call `_set_years()` first to select a specific vintage."
                )
                return
            found_product = matching_products[0]

        self.product = found_product
        self.product["base_url"] = self.product.get("url", "")
        print(
            f"✅ Product set to: '{self.product['title']}' (Vintage(s): {self.product.get('vintage')})"
        )

    def list_geos(
        self, to_dicts: bool = False
    ) -> Union[List[str], List[Dict[str, str]]]:
        """Lists available geographies for the currently set product."""
        if not self.product or not self.product.get("base_url"):
            print("❌ Error: A product must be set first via `set_product()`.")
            return []

        url = f"{self.product['base_url']}/geography.json"
        data = self._get_json_from_url(url)
        if not data or "fips" not in data:
            return []

        geos = []
        for geo_info in data["fips"]:
            geos.append(
                {
                    "sumlev": geo_info.get("geoLevelDisplay"),
                    "desc": geo_info.get("name"),
                    "refdate": geo_info.get("referenceDate"),
                }
            )

        return geos if to_dicts else [g["sumlev"] for g in geos]

    def list_variables(
        self,
        to_dicts: bool = True,
        pattern: Optional[Union[str, List[str]]] = None,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available variables for the currently set product.

        Args:
            to_dicts (bool): If True (default), returns a list of dictionaries.
                             If False, returns a list of variable names.
            pattern (str | list[str], optional): A regex pattern or list of
                patterns to filter variables by label.
            logic (callable): `all` (default) or `any`. Determines if all
                              patterns must match or if any can match.
        """
        if not self.product or not self.product.get("base_url"):
            print("❌ Error: A product must be set first via `set_product()`.")
            return []

        url = f"{self.product['base_url']}/variables.json"
        data = self._get_json_from_url(url)
        if not data or "variables" not in data:
            return []

        variables = []
        for name, details in data["variables"].items():
            if name in ["GEO_ID", "for", "in"]:
                continue
            variables.append(
                {
                    "name": name,
                    "label": details.get("label"),
                    "concept": details.get("concept"),
                    "group": details.get("group", "N/A"),
                }
            )

        if pattern:
            patterns = [pattern] if isinstance(pattern, str) else pattern
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in patterns]
                variables = [
                    v
                    for v in variables
                    if v.get("label")
                    and logic(regex.search(v["label"]) for regex in regexes)
                ]
            except re.error as e:
                print(f"❌ Invalid regex pattern: {e}")
                return []

        return variables if to_dicts else [v["name"] for v in variables]
