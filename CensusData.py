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
        products (list[dict]): A list of the currently selected data product details.
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
        self.products: List[Dict] = []
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
        patterns: Optional[Union[str, List[str]]] = None,
        to_dicts: bool = True,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available data products from the JSON endpoint.

        Args:
            years (int | list[int], optional): Filters products for these years.
            patterns (str | list[str], optional): A regex pattern or list of
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

        if patterns:
            pattern_list = [patterns] if isinstance(patterns, str) else patterns
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in pattern_list]
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

    def set_products(self, titles: Union[str, List[str]]):
        """
        Sets the active data products for the object.

        Args:
            titles (str | list[str]): A single product title or a list of titles.
        """
        title_list = [titles] if isinstance(titles, str) else titles
        all_prods = self.list_products(to_dicts=True, years=self.years or [])

        self.products = []
        for title in title_list:
            matching_products = [p for p in all_prods if p.get("title") == title]
            if not matching_products:
                print(
                    f"⚠️ Warning: No product with the title '{title}' found for the specified years. Skipping."
                )
                continue

            for product in matching_products:
                product["base_url"] = product.get("url", "")
                self.products.append(product)
                print(
                    f"✅ Product set: '{product['title']}' (Vintage(s): {product.get('vintage')})"
                )

        if not self.products:
            print("❌ Error: No valid products were set.")

    def list_geos(
        self, to_dicts: bool = False
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available geographies across all currently set products.

        Returns a unique list of geographies. If to_dicts is True, each
        dictionary includes an 'applies_to' key detailing which products
        and vintages it belongs to.
        """
        if not self.products:
            print("❌ Error: Products must be set first via `set_products()`.")
            return []

        combined_geos = {}
        for product in self.products:
            url = f"{product['base_url']}/geography.json"
            data = self._get_json_from_url(url)
            if not data or "fips" not in data:
                continue

            for geo_info in data["fips"]:
                sumlev = geo_info.get("geoLevelDisplay")
                if not sumlev:
                    continue

                applies_to_info = {
                    "product": product["title"],
                    "years": product["vintage"],
                }

                if sumlev not in combined_geos:
                    combined_geos[sumlev] = {
                        "sumlev": sumlev,
                        "desc": geo_info.get("name"),
                        "applies_to": [applies_to_info],
                    }
                else:
                    combined_geos[sumlev]["applies_to"].append(applies_to_info)

        result_list = list(combined_geos.values())

        # Consolidate the 'applies_to' field for each item in the results
        if to_dicts:
            for item in result_list:
                consolidated_applies_to = {}
                for applies_info in item["applies_to"]:
                    title = applies_info["product"]
                    years = applies_info["years"]
                    if title not in consolidated_applies_to:
                        consolidated_applies_to[title] = []
                    consolidated_applies_to[title].extend(years)

                item["applies_to"] = [
                    {"product": title, "years": sorted(list(set(years)))}
                    for title, years in consolidated_applies_to.items()
                ]

        return result_list if to_dicts else [g["sumlev"] for g in result_list]

    def list_variables(
        self,
        to_dicts: bool = True,
        patterns: Optional[Union[str, List[str]]] = None,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available variables across all currently set products.

        Returns a unique list of variables. If to_dicts is True, each
        dictionary includes an 'applies_to' key detailing which products
        and vintages it belongs to.

        Args:
            to_dicts (bool): If True (default), returns a list of dictionaries.
                             If False, returns a list of variable names.
            patterns (str | list[str], optional): A regex pattern or list of
                patterns to filter variables by label.
            logic (callable): `all` (default) or `any`. Determines if all
                              patterns must match or if any can match.
        """
        if not self.products:
            print("❌ Error: Products must be set first via `set_products()`.")
            return []

        combined_vars = {}
        for product in self.products:
            url = f"{product['base_url']}/variables.json"
            data = self._get_json_from_url(url)
            if not data or "variables" not in data:
                continue

            for name, details in data["variables"].items():
                if name in ["GEO_ID", "for", "in"]:
                    continue

                applies_to_info = {
                    "product": product["title"],
                    "years": product["vintage"],
                }

                if name not in combined_vars:
                    combined_vars[name] = {
                        "name": name,
                        "label": details.get("label"),
                        "concept": details.get("concept"),
                        "group": details.get("group", "N/A"),
                        "applies_to": [applies_to_info],
                    }
                else:
                    combined_vars[name]["applies_to"].append(applies_to_info)

        # Filter after combining
        result_list = list(combined_vars.values())
        if patterns:
            pattern_list = [patterns] if isinstance(patterns, str) else patterns
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in pattern_list]
                result_list = [
                    v
                    for v in result_list
                    if v.get("label")
                    and logic(regex.search(v["label"]) for regex in regexes)
                ]
            except re.error as e:
                print(f"❌ Invalid regex pattern: {e}")
                return []

        # Consolidate the 'applies_to' field for each item in the results
        if to_dicts:
            for item in result_list:
                consolidated_applies_to = {}
                for applies_info in item["applies_to"]:
                    title = applies_info["product"]
                    years = applies_info["years"]
                    if title not in consolidated_applies_to:
                        consolidated_applies_to[title] = []
                    consolidated_applies_to[title].extend(years)

                item["applies_to"] = [
                    {"product": title, "years": sorted(list(set(years)))}
                    for title, years in consolidated_applies_to.items()
                ]

        return result_list if to_dicts else [v["name"] for v in result_list]
