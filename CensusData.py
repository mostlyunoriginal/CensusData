import re
import requests
from typing import List, Union, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed


class CenDatResponse:
    """
    A container for the data returned by the CenDatHelper.get_data() method.
    Provides methods to easily convert the raw data into Polars or Pandas DataFrames.
    """

    def __init__(self, data: List[Dict]):
        self._data = data

    def to_polars(
        self, schema_overrides: Optional[Dict] = None
    ) -> List["pl.DataFrame"]:
        """
        Converts the response data into a list of Polars DataFrames.

        Args:
            schema_overrides (dict, optional): A dictionary to override inferred schema types.
                                                Passed directly to polars.DataFrame().
                                                Example: {'POP': pl.Int64, 'GEO_ID': pl.Utf8}
        """
        try:
            import polars as pl
        except ImportError:
            print(
                "❌ Polars is not installed. Please install it with 'pip install polars'"
            )
            return []

        dataframes = []
        for item in self._data:
            if not item.get("data"):
                continue  # Skip if no data was returned for this parameter set

            df = pl.DataFrame(
                item["data"],
                schema=item["schema"],
                orient="row",
                schema_overrides=schema_overrides,
            )

            # Add context columns
            df = df.with_columns(
                [
                    pl.lit(item["product"]).alias("product"),
                    pl.lit(item["vintage"][0]).alias("vintage"),
                    pl.lit(item["sumlev"]).alias("sumlev"),
                    pl.lit(item["desc"]).alias("desc"),
                ]
            )
            dataframes.append(df)
        return dataframes

    def to_pandas(self, dtypes: Optional[Dict] = None) -> List["pd.DataFrame"]:
        """
        Converts the response data into a list of Pandas DataFrames.

        Args:
            dtypes (dict, optional): A dictionary of column names to data types,
                                     passed to the pandas.DataFrame.astype() method.
                                     Example: {'POP': 'int64', 'GEO_ID': 'str'}
        """
        try:
            import pandas as pd
        except ImportError:
            print(
                "❌ Pandas is not installed. Please install it with 'pip install pandas'"
            )
            return []

        dataframes = []
        for item in self._data:
            if not item.get("data"):
                continue  # Skip if no data was returned for this parameter set

            df = pd.DataFrame(item["data"], columns=item["schema"])

            if dtypes:
                df = df.astype(dtypes, errors="ignore")

            # Add context columns
            df["product"] = item["product"]
            df["vintage"] = item["vintage"][0]
            df["sumlev"] = item["sumlev"]
            df["desc"] = item["desc"]
            dataframes.append(df)
        return dataframes

    def __repr__(self) -> str:
        return f"<CenDatResponse with {len(self._data)} result(s)>"

    def __getitem__(self, index: int) -> Dict:
        """Allows accessing individual results by index."""
        return self._data[index]


class CenDatHelper:
    """
    A helper object for exploring and working with the US Census Bureau API.

    This class provides methods to list and select datasets, geographies, and
    variables by interacting directly with the Census JSON API endpoints.

    Attributes:
        years (list[int]): The primary year or years of interest for data queries.
        products (list[dict]): A list of the currently selected data product details.
        geos (list[dict]): A list of the currently selected geographies.
        variables (list[dict]): A list of the currently selected variables.
        params (list[dict]): A list of combined geography and variable parameters for API calls.
    """

    def __init__(
        self, years: Optional[Union[int, List[int]]] = None, key: Optional[str] = None
    ):
        """
        Initializes the CenDatHelper object.

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
        self.params: List[Dict] = []
        self.__key: Optional[str] = None
        self._products_cache: Optional[List[Dict[str, str]]] = None
        self._filtered_products_cache: Optional[List[Dict]] = None
        self._filtered_geos_cache: Optional[List[Dict]] = None
        self._filtered_variables_cache: Optional[List[Dict]] = None

        if years is not None:
            self.set_years(years)
        if key is not None:
            self.load_key(key)

    def __getitem__(self, key: str) -> List[Dict]:
        """Allows accessing key attributes by name."""
        if key == "products":
            return self.products
        elif key == "geos":
            return self.geos
        elif key == "variables":
            return self.variables
        elif key == "params":
            return self.params
        else:
            raise KeyError(
                f"'{key}' is not a valid key. Available keys are: 'products', 'geos', 'variables', 'params'"
            )

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
        except requests.exceptions.JSONDecodeError as e:
            print(
                f"❌ Failed to decode JSON from {url}. Server response: {response.text}"
            )
        except requests.exceptions.RequestException as e:
            error_message = str(e)
            if e.response is not None:
                api_error = e.response.text.strip()
                if api_error:
                    error_message += f" - API Message: {api_error}"
            print(
                f"❌ Error fetching data from {url} with params {params}: {error_message}"
            )
        return None

    def _parse_vintage(self, vintage_input: Union[str, int]) -> List[int]:
        """
        Robustly parses a vintage value.
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
        match_in: str = "title",
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available data products from the JSON endpoint.
        """
        if not self._products_cache:
            data = self._get_json_from_url("https://api.census.gov/data.json")
            if not data or "dataset" not in data:
                return []
            products = []
            for d in data["dataset"]:
                is_micro = str(d.get("c_isMicrodata", "false")).lower() == "true"
                is_agg = str(d.get("c_isAggregate", "false")).lower() == "true"
                if not is_micro and not is_agg:
                    continue

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
                    dataset_type = "/".join(c_dataset_val)
                elif isinstance(c_dataset_val, str):
                    dataset_type = c_dataset_val

                title = d.get("title")
                title = (
                    f"{title} ({re.sub(r'http://api.census.gov/data/','', access_url)})"
                )

                products.append(
                    {
                        "title": title,
                        "desc": d.get("description"),
                        "vintage": self._parse_vintage(d.get("c_vintage")),
                        "type": dataset_type,
                        "url": access_url,
                        "is_microdata": is_micro,
                        "is_aggregate": is_agg,
                    }
                )
            self._products_cache = products

        target_years = self.years
        if years is not None:
            target_years = [years] if isinstance(years, int) else list(years)

        filtered = self._products_cache
        if target_years:
            target_set = set(target_years)
            filtered = [
                p
                for p in filtered
                if p.get("vintage") and target_set.intersection(p["vintage"])
            ]

        if patterns:
            if match_in not in ["title", "desc"]:
                print("❌ Error: `match_in` must be either 'title' or 'desc'.")
                return []
            pattern_list = [patterns] if isinstance(patterns, str) else patterns
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in pattern_list]
                filtered = [
                    p
                    for p in filtered
                    if p.get(match_in)
                    and logic(regex.search(p[match_in]) for regex in regexes)
                ]
            except re.error as e:
                print(f"❌ Invalid regex pattern: {e}")
                return []

        self._filtered_products_cache = filtered
        return filtered if to_dicts else [p["title"] for p in filtered]

    def set_products(self, titles: Optional[Union[str, List[str]]] = None):
        """
        Sets the active data products.
        """
        prods_to_set = []
        if titles is None:
            if not self._filtered_products_cache:
                print("❌ Error: No products to set. Run `list_products` first.")
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
                        "url": product["url"],
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

    def set_geos(
        self,
        values: Optional[Union[str, List[str]]] = None,
        by: str = "sumlev",
    ):
        """
        Sets the active geographies, informing the user of any required parent geos.

        Args:
            values (str or list, optional): The geography values to set.
                                            Can be summary levels or descriptions.
                                            If None, sets all geos from the last `list_geos` call.
            by (str, optional): The key to use for matching values.
                                Must be either 'sumlev' (default) or 'desc'.
        """
        if by not in ["sumlev", "desc"]:
            print("❌ Error: `by` must be either 'sumlev' or 'desc'.")
            return

        geos_to_set = []
        if values is None:
            if not self._filtered_geos_cache:
                print("❌ Error: No geos to set. Run `list_geos` first.")
                return
            geos_to_set = self._filtered_geos_cache
        else:
            value_list = [values] if isinstance(values, str) else values
            all_geos = self.list_geos(to_dicts=True)
            geos_to_set = [g for g in all_geos if g.get(by) in value_list]

        if not geos_to_set:
            print("❌ Error: No valid geographies were found to set.")
            return

        is_microdata_present = any(
            p.get("is_microdata")
            for p in self.products
            if p["title"] in [g["product"] for g in geos_to_set]
        )

        unique_geos = set(g["desc"] for g in geos_to_set)
        if is_microdata_present and len(unique_geos) > 1:
            print(
                "❌ Error: Only a single geography type (e.g., 'public use microdata area') can be set when working with microdata products."
            )
            return

        self.geos = geos_to_set
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
        match_in: str = "label",
    ) -> Union[List[str], List[Dict[str, str]]]:
        """
        Lists available variables across all currently set products.
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
                if name in ["GEO_ID", "for", "in", "ucgid"]:
                    continue
                flat_variable_list.append(
                    {
                        "name": name,
                        "label": details.get("label", "N/A"),
                        "concept": details.get("concept", "N/A"),
                        "group": details.get("group", "N/A"),
                        "values": details.get("values", "N/A"),
                        "type": details.get("predicateType", "N/A"),
                        "sugg_wgt": details.get("suggested-weight", "N/A"),
                        "product": product["title"],
                        "vintage": product["vintage"],
                        "url": product["url"],
                    }
                )
        result_list = flat_variable_list

        if match_in not in ["label", "name"]:
            print("❌ Error: `match_in` must be either 'label' or 'name'.")
            return []

        if patterns:
            pattern_list = [patterns] if isinstance(patterns, str) else patterns
            try:
                regexes = [re.compile(p, re.IGNORECASE) for p in pattern_list]
                result_list = [
                    v
                    for v in result_list
                    if v.get(match_in)
                    and logic(regex.search(v[match_in]) for regex in regexes)
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
        """
        vars_to_set = []
        if names is None:
            if not self._filtered_variables_cache:
                print("❌ Error: No variables to set. Run `list_variables` first.")
                return
            vars_to_set = self._filtered_variables_cache
        else:
            name_list = [names] if isinstance(names, str) else names
            all_vars = self.list_variables(to_dicts=True, patterns=None)
            vars_to_set = [v for v in all_vars if v.get("name") in name_list]
        if not vars_to_set:
            print("❌ Error: No valid variables were found to set.")
            return
        collapsed_vars = {}
        for var_info in vars_to_set:
            key = (var_info["product"], tuple(var_info["vintage"]), var_info["url"])
            if key not in collapsed_vars:
                collapsed_vars[key] = {
                    "product": var_info["product"],
                    "vintage": var_info["vintage"],
                    "url": var_info["url"],
                    "names": [],
                    "labels": [],
                    "values": [],
                    "types": [],
                    "sugg_wgts": [],
                }
            for collapsed, granular in zip(
                ["names", "labels", "values", "types", "sugg_wgts"],
                ["name", "label", "values", "type", "sugg_wgt"],
            ):
                collapsed_vars[key][collapsed].append(var_info[granular])
        self.variables = list(collapsed_vars.values())
        print(f"✅ Variables set:")
        for var_group in self.variables:
            print(
                f"  - Product: {var_group['product']} (Vintage: {var_group['vintage']})"
            )
            print(f"    Variables: {', '.join(var_group['names'])}")

    def _create_params(self):
        """
        Internal method to join the set geos and variables by product and vintage.
        """
        if not self.geos or not self.variables:
            print(
                "❌ Error: Geographies and variables must be set before creating parameters."
            )
            return
        self.params = []
        for geo in self.geos:
            for var_group in self.variables:
                if (
                    geo["product"] == var_group["product"]
                    and geo["vintage"] == var_group["vintage"]
                    and geo["url"] == var_group["url"]
                ):
                    self.params.append(
                        {
                            "product": geo["product"],
                            "vintage": geo["vintage"],
                            "sumlev": geo["sumlev"],
                            "desc": geo["desc"],
                            "requires": geo.get("requires"),
                            "names": var_group["names"],
                            "labels": var_group["labels"],
                            "values": var_group["values"],
                            "types": var_group["types"],
                            "url": geo["url"],
                        }
                    )
        if not self.params:
            print(
                "⚠️ Warning: No matching product-vintage combinations found between set geos and variables."
            )
        else:
            print(
                f"✅ Parameters created for {len(self.params)} geo-variable combinations."
            )

    def _get_parent_geo_combinations(
        self, base_url: str, required_geos: List[str], current_in_clause: Dict = {}
    ) -> List[Dict]:
        """
        Recursively fetches all valid combinations of parent geographies for aggregate data.
        """
        if not required_geos:
            return [current_in_clause]
        level_to_fetch = required_geos[0]
        remaining_levels = required_geos[1:]
        params = {"get": "NAME", "for": f"{level_to_fetch}:*"}
        if current_in_clause:
            params["in"] = " ".join([f"{k}:{v}" for k, v in current_in_clause.items()])
        data = self._get_json_from_url(base_url, params)
        if not data or len(data) < 2:
            return []
        try:
            fips_index = data[0].index(level_to_fetch)
        except ValueError:
            print(
                f"❌ Could not find FIPS column for '{level_to_fetch}' in API response."
            )
            return []
        all_combinations = []
        with ThreadPoolExecutor() as executor:
            future_to_fips = {
                executor.submit(
                    self._get_parent_geo_combinations,
                    base_url,
                    remaining_levels,
                    {**current_in_clause, level_to_fetch: row[fips_index]},
                ): row[fips_index]
                for row in data[1:]
            }
            for future in as_completed(future_to_fips):
                all_combinations.extend(future.result())
        return all_combinations

    def get_data(
        self,
        within: Union[str, Dict, List[Dict]] = "us",
        max_workers: Optional[int] = 100,
    ) -> "CenDatResponse":
        """
        Retrieves data and returns a CenDatResponse object for further processing.
        """
        self._create_params()

        if not self.params:
            print(
                "❌ Error: Could not create parameters. Please set geos and variables."
            )
            return CenDatResponse([])

        results_aggregator = {
            i: {"schema": None, "data": []} for i in range(len(self.params))
        }
        all_tasks = []

        within_clauses = within if isinstance(within, list) else [within]

        for i, param in enumerate(self.params):
            product_info = next(
                (p for p in self.products if p["title"] == param["product"]), None
            )
            if not product_info:
                continue

            variable_names = ",".join(param["names"])
            target_geo = param["desc"]
            vintage_url = param["url"]
            context = {"param_index": i}

            for within_clause in within_clauses:
                if product_info.get("is_microdata"):
                    if not isinstance(within_clause, dict):
                        print(
                            "❌ Error: A `within` dictionary or list of dictionaries is required for microdata requests."
                        )
                        continue

                    within_copy = within_clause.copy()
                    target_geo_codes = within_copy.pop(target_geo, None)

                    if target_geo_codes is None:
                        print(
                            f"❌ Error: `within` dictionary must contain the target geography: '{target_geo}'"
                        )
                        continue

                    if isinstance(target_geo_codes, str):
                        target_geo_codes = [target_geo_codes]

                    codes_str = ",".join(target_geo_codes)
                    api_params = {
                        "get": variable_names,
                        "for": f"{target_geo}:{codes_str}",
                    }
                    if within_copy:
                        api_params["in"] = " ".join(
                            [f"{k}:{v}" for k, v in within_copy.items()]
                        )
                    all_tasks.append((vintage_url, api_params, context))

                elif product_info.get("is_aggregate"):
                    # --- CHANGE START: Optimized geo combination logic ---
                    required_geos = param.get("requires") or []
                    
                    # Determine which parent geos are already provided in the within_clause
                    provided_geos = {}
                    if isinstance(within_clause, dict):
                        provided_geos = {k: v for k, v in within_clause.items() if k in required_geos}

                    # Determine which parent geos still need to be fetched
                    geos_to_fetch = [g for g in required_geos if g not in provided_geos]
                    
                    print(f"ℹ️ Fetching parent geographies for '{param['desc']}'...")
                    combinations = self._get_parent_geo_combinations(vintage_url, geos_to_fetch, provided_geos)
                    print(f"✅ Found {len(combinations)} combinations for '{param['desc']}' within the specified scope.")

                    for combo in combinations:
                        api_params = {"get": variable_names}
                        if combo:
                            api_params["in"] = " ".join(
                                [f"{k}:{v}" for k, v in combo.items()]
                            )
                        api_params["for"] = f"{target_geo}:*"
                        all_tasks.append((vintage_url, api_params, context))
                    # --- CHANGE END ---

        if not all_tasks:
            print("❌ Error: Could not determine any API calls to make.")
            return CenDatResponse([])

        print(f"ℹ️ Making {len(all_tasks)} API call(s)...")
        with ThreadPoolExecutor(max_workers=max_workers or len(all_tasks)) as executor:
            future_to_context = {
                executor.submit(self._get_json_from_url, url, params): context
                for url, params, context in all_tasks
            }
            for future in as_completed(future_to_context):
                context = future_to_context[future]
                param_index = context["param_index"]
                try:
                    data = future.result()
                    if data and len(data) > 1:
                        if results_aggregator[param_index]["schema"] is None:
                            results_aggregator[param_index]["schema"] = data[0]
                        results_aggregator[param_index]["data"].extend(data[1:])
                except Exception as exc:
                    print(f"❌ Task for {context} generated an exception: {exc}")

        for i, param in enumerate(self.params):
            aggregated_result = results_aggregator[i]
            param["schema"] = aggregated_result["schema"]
            param["data"] = aggregated_result["data"]

        return CenDatResponse(self.params)
