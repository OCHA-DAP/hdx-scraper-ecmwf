#!/usr/bin/python
"""Ecmwf scraper"""

import logging
from datetime import datetime
from os.path import basename, exists, join
from typing import Dict, List, Optional
from zipfile import ZipFile

import cdsapi
from dateutil.relativedelta import relativedelta
from geopandas import list_layers, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.global_boundaries = None
        self.grib_data = []

    def download_global_boundaries(self) -> Dict:
        zip_file_path = self._retriever.download_file(
            self._configuration["global_boundaries"]
        )
        gdb_file_path = join(
            self._tempdir, basename(zip_file_path).replace("-gdb.zip", ".gdb")
        )
        with ZipFile(zip_file_path, "r") as z:
            z.extractall(gdb_file_path)
        layers = list_layers(gdb_file_path)
        layer_name = [layer for layer in layers.name if "1" in layer]
        adm1_data = read_file(gdb_file_path, layer=layer_name[0])
        self.global_boundaries = adm1_data
        adm0_codes = list(set(adm1_data["adm0_pcode"]))
        iso_codes = {}
        for code in adm0_codes:
            if len(code) > 3:
                code = code[:2]
            country_name = None
            iso_code = None
            if len(code) == 3:
                iso_code = code
                country_name = Country.get_country_name_from_iso3(code)
            if len(code) == 2:
                iso_code = Country.get_iso3_from_iso2(code)
                country_name = Country.get_country_name_from_iso2(code)
            if not country_name:
                logger.error(f"No country name found for {code}")
            iso_codes[code] = {"iso3": iso_code, "name": country_name}
        return iso_codes

    def download_rasters(
        self, cds_key: str, today: datetime, force_refresh: bool = False
    ) -> bool:
        existing_data = _get_uploaded_data(force_refresh)
        if self._retriever.save or self._retriever.use_saved:
            root_dir = self._retriever.saved_dir
        else:
            root_dir = self._tempdir
        dataset = "seasonal-monthly-single-levels"

        for year in range(self._configuration["min_year"], today.year + 1):
            for month in range(1, 13):
                if year == today.year and month > today.month:
                    continue
                str_month = str(month).zfill(2)
                if f"{year}_{str_month}" in existing_data:
                    continue
                file_name = f"{dataset}-{year}-{month}.grib"
                filepath = join(root_dir, file_name)
                request = {
                    "originating_centre": "ecmwf",
                    "system": "51",
                    "variable": ["total_precipitation"],
                    "product_type": ["ensemble_mean"],
                    "year": str(year),
                    "month": str_month,
                    "leadtime_month": ["1", "2", "3", "4", "5", "6"],
                    "data_format": "grib",
                }
                if self._retriever.save and exists(filepath):
                    self.grib_data.append(filepath)
                    continue
                client = cdsapi.Client(url=self._configuration["cds_url"], key=cds_key)
                client.retrieve(dataset, request, filepath)
                self.grib_data.append(filepath)
        if len(self.grib_data) > 0:
            return True
        return False

    def generate_dataset(self, country_info: Dict) -> Optional[Dataset]:
        country_name = country_info["name"]
        country_iso = country_info["iso3"]
        dataset_name = f"ecmwf-{country_iso.lower()}"
        dataset_title = f"{country_name}: ECMWF"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        start_date = f"{self._configuration['min_year']}-01-01"
        end_date = sorted(self.grib_data)[-1][-12:-5]
        end_date = parse_date(f"{end_date.replace('_', '-')}-01")
        end_date = end_date + relativedelta(day=31)
        dataset.set_time_period(startdate=start_date, enddate=end_date)

        dataset.add_tags(self._configuration["tags"])
        dataset.add_country_location(country_iso)

        # Add resources here

        return dataset


def _get_uploaded_data(force_refresh: bool) -> List:
    uploaded_data = []
    if force_refresh:
        return uploaded_data
    dataset = Dataset.read_from_hdx("ecmwf-afg")
    if not dataset:
        return uploaded_data
    resources = dataset.get_resources()
    for resource in resources:
        uploaded_data.append(resource["name"][-11:-4])
    return uploaded_data
