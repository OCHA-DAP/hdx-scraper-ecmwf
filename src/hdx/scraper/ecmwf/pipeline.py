#!/usr/bin/python
"""Ecmwf scraper"""

import logging
from os.path import basename, join
from typing import Dict, Optional
from zipfile import ZipFile

from geopandas import list_layers, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.global_boundaries = None

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
            iso_codes[code] = [iso_code, country_name]
        return iso_codes

    def download_rasters(self):
        return

    def generate_dataset(self) -> Optional[Dataset]:
        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset
