#!/usr/bin/python
"""Ecmwf scraper"""

import logging
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

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
