import os
import re
import requests
from typing import List, Union, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed


class CensusData:
    """
    A helper object for exploring and working with the US Census Bureau API.

    This class provides methods to list and select datasets, geographies, and
    variables by interacting directly with the Census JSON API endpoints.

    Attributes:
        years (list[int]): The primary year or years of interest for data queries.
        products (list[dict]): A list of the currently selected data product details.
        geos (list[dict]): A list of the currently selected geographies.
        variables (list[dict]): A list of the currently selected variables.
    """

    def __init__(
        self, years: Optional[Union[int, List[int]]] = None, key: Optional[str] = None
    ):
        """
        Initializes the CensusData object.

        Args:
            years (int | list[int], optional): The year or years of interest.
                                               If provided, they are set upon
                                               initialization. Defaults to None.
            key (str, optional): An API key to load upon initialization.
        """
        self.years: Optional[List[int]] = None
        self.products: List[Dict] = []
        self.geos: List[Dict] = []
        self.variables: List[Dict] = []
        self.__key: Optional[str] = None
        self._products_cache: Optional[List[Dict[str, str]]] = None
        self._filtered_products_cache: Optional[List[Dict]] = None
        self._filtered_geos_cache: Optional[List[Dict]] = None
        self._filtered_variables_cache: Optional[List[Dict]] = None

        if years is not None:
            self.set_years(years)
        if key is not None:
            self.load_key(key)

    def set_years(self, years: Union[int, List[int]]):
        """Sets the object's years attribute."""
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

    def _get_json_from_url(
        self, url: str, params: Optional[Dict] = None
    ) -> Optional[List[List[str]]]:
        """Helper to fetch and parse JSON from a URL."""
        if not params:
            params = {}
        if self.__key:
            params["key"] = self.__key

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_message = str(e)
            if e.response is not None:
                # Attempt to include the API's specific error message for better debugging.
                api_error = e.response.text.strip()
                if api_error:
                    error_message += f" - API Message: {api_error}"
            print(
                f"❌ Error fetching data from {url} with params {params}: {error_message}"
            )
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
        Lists available data products from the JSON endpoint. The results of this
        call are cached and can be applied by calling `set_products()` with no arguments.

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
                        "is_microdata": str(d.get("c_isMicrodata", "false")).lower()
                        == "true",
                        "is_aggregate": str(d.get("c_isAggregate", "false")).lower()
                        == "true",
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

        self._filtered_products_cache = filtered
        return filtered if to_dicts else [p["title"] for p in filtered]

    def set_products(self, titles: Optional[Union[str, List[str]]] = None):
        """
        Sets the active data products. If titles are provided, sets those specific
        products. If no titles are provided, sets all products from the last
        `list_products` call.

        Args:
            titles (str | list[str], optional): A single product title or a list of titles.
        """
        prods_to_set = []
        if titles is None:
            if not self._filtered_products_cache:
                print(
                    "❌ Error: No products to set. Run `list_products` with your desired filters first."
                )
                return
            prods_to_set = self._filtered_products_cache
        else:
            title_list = [titles] if isinstance(titles, str) else titles
            all_prods = self.list_products(to_dicts=True, years=self.years or [])

            for title in title_list:
                matching_products = [p for p in all_prods if p.get("title") == title]
                if not matching_products:
                    print(
                        f"⚠️ Warning: No product with the title '{title}' found. Skipping."
                    )
                    continue
                prods_to_set.extend(matching_products)

        self.products = []
        if not prods_to_set:
            print("❌ Error: No valid products were found to set.")
            return

        for product in prods_to_set:
            product["base_url"] = product.get("url", "")
            self.products.append(product)
            print(
                f"✅ Product set: '{product['title']}' (Vintage: {product.get('vintage')})"
            )

    def list_geos(
        self,
        to_dicts: bool = False,
        patterns: Optional[Union[str, List[str]]] = None,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available geographies across all currently set products.

        Args:
            to_dicts (bool): If True, returns a list of dictionaries with geo details.
            patterns (str | list[str], optional): Filters geos by their description.
            logic (callable): `all` or `any` logic for pattern matching.
        """
        if not self.products:
            print("❌ Error: Products must be set first via `set_products()`.")
            return []

        flat_geo_list = []
        for product in self.products:
            url = f"{product['base_url']}/geography.json"
            data = self._get_json_from_url(url)
            if not data or "fips" not in data:
                continue

            for geo_info in data["fips"]:
                sumlev = geo_info.get("geoLevelDisplay")
                if not sumlev:
                    continue

                flat_geo_list.append(
                    {
                        "sumlev": sumlev,
                        "desc": geo_info.get("name"),
                        "product": product["title"],
                        "vintage": product["vintage"],
                        "requires": geo_info.get("requires"),
                    }
                )

        result_list = flat_geo_list
        if patterns:
            pattern_list = [patterns] if isinstance(patterns, str) else patterns
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in pattern_list]
                result_list = [
                    g
                    for g in result_list
                    if g.get("desc")
                    and logic(regex.search(g["desc"]) for regex in regexes)
                ]
            except re.error as e:
                print(f"❌ Invalid regex pattern: {e}")
                return []

        self._filtered_geos_cache = result_list
        return (
            result_list
            if to_dicts
            else sorted(list(set([g["sumlev"] for g in result_list])))
        )

    def set_geos(self, sumlevs: Optional[Union[str, List[str]]] = None):
        """
        Sets the active geographies and informs the user of any required parent geos.

        Args:
            sumlevs (str | list[str], optional): A single geography sumlev or a list of them.
                If None, sets all geos from the last `list_geos` call.
        """
        geos_to_set = []
        if sumlevs is None:
            if not self._filtered_geos_cache:
                print(
                    "❌ Error: No geos to set. Run `list_geos` with your desired filters first."
                )
                return
            geos_to_set = self._filtered_geos_cache
        else:
            sumlev_list = [sumlevs] if isinstance(sumlevs, str) else sumlevs
            all_geos = self.list_geos(to_dicts=True)
            geos_to_set = [g for g in all_geos if g.get("sumlev") in sumlev_list]

        if not geos_to_set:
            print("❌ Error: No valid geographies were found to set.")
            return

        self.geos = geos_to_set

        # Build informative success message by consolidating requirements across all set geos
        messages = {}
        for geo in self.geos:
            desc = geo["desc"]
            reqs = geo.get("requires") or []
            if desc not in messages:
                messages[desc] = set(reqs)
            else:
                messages[desc].update(reqs)

        message_parts = []
        for desc, reqs in messages.items():
            if reqs:
                message_parts.append(
                    f"'{desc}' (requires `within` for: {', '.join(sorted(list(reqs)))})"
                )
            else:
                message_parts.append(f"'{desc}'")
        print(f"✅ Geographies set: {', '.join(message_parts)}")

    def list_variables(
        self,
        to_dicts: bool = True,
        patterns: Optional[Union[str, List[str]]] = None,
        logic: Callable[[iter], bool] = all,
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available variables across all currently set products.

        Args:
            to_dicts (bool): If True (default), returns a list of dictionaries.
            patterns (str | list[str], optional): Filters variables by their label.
            logic (callable): `all` or `any` logic for pattern matching.
        """
        if not self.products:
            print("❌ Error: Products must be set first via `set_products()`.")
            return []

        flat_variable_list = []
        for product in self.products:
            url = f"{product['base_url']}/variables.json"
            data = self._get_json_from_url(url)
            if not data or "variables" not in data:
                continue

            for name, details in data["variables"].items():
                if name in ["GEO_ID", "for", "in"]:
                    continue

                flat_variable_list.append(
                    {
                        "name": name,
                        "label": details.get("label"),
                        "concept": details.get("concept"),
                        "group": details.get("group", "N/A"),
                        "product": product["title"],
                        "vintage": product["vintage"],
                    }
                )

        result_list = flat_variable_list
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

        self._filtered_variables_cache = result_list
        return (
            result_list
            if to_dicts
            else sorted(list(set([v["name"] for v in result_list])))
        )

    def set_variables(self, names: Optional[Union[str, List[str]]] = None):
        """
        Sets the active variables, grouping them by product and vintage.

        Args:
            names (str | list[str], optional): A single variable name or a list of them.
                If None, sets all variables from the last `list_variables` call.
        """
        vars_to_set = []
        if names is None:
            if not self._filtered_variables_cache:
                print(
                    "❌ Error: No variables to set. Run `list_variables` with your desired filters first."
                )
                return
            vars_to_set = self._filtered_variables_cache
        else:
            name_list = [names] if isinstance(names, str) else names
            all_vars = self.list_variables(to_dicts=True)
            vars_to_set = [v for v in all_vars if v.get("name") in name_list]

        if not vars_to_set:
            print("❌ Error: No valid variables were found to set.")
            return

        # Collapse the flat list into the desired structure
        collapsed_vars = {}
        for var_info in vars_to_set:
            # Create a hashable key for each product-vintage combination
            key = (var_info["product"], tuple(var_info["vintage"]))
            if key not in collapsed_vars:
                collapsed_vars[key] = {
                    "product": var_info["product"],
                    "vintage": var_info["vintage"],
                    "names": [],
                }
            collapsed_vars[key]["names"].append(var_info["name"])

        self.variables = list(collapsed_vars.values())

        print(f"✅ Variables set:")
        for var_group in self.variables:
            print(
                f"  - Product: {var_group['product']} (Vintage: {var_group['vintage']})"
            )
            print(f"    Variables: {', '.join(var_group['names'])}")
