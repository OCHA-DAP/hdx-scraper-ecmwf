#!/usr/bin/python
"""Ecmwf scraper"""

import logging
from datetime import datetime
from os.path import basename, exists, join
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

import cdsapi
import xarray as xr
from dateutil.relativedelta import relativedelta
from geopandas import read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from numpy import datetime_as_string
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.global_boundaries = {}
        self.grib_data = []

    def download_global_boundaries(self) -> None:
        zip_file_path = self._retriever.download_file(
            self._configuration["global_boundaries"]
        )
        gdb_file_path = join(
            self._tempdir, basename(zip_file_path).replace("-gdb.zip", ".gdb")
        )
        with ZipFile(zip_file_path, "r") as z:
            z.extractall(gdb_file_path)
        for admin_level in ["0", "1"]:
            adm_data = read_file(gdb_file_path, layer=f"adm{admin_level}")
            self.global_boundaries[admin_level] = adm_data

    def download_rasters(
        self, cds_key: str, today: datetime, force_refresh: bool = False
    ) -> bool:
        existing_data, dataset_month = _get_uploaded_data(force_refresh)
        if self._retriever.save or self._retriever.use_saved:
            root_dir = self._retriever.saved_dir
        else:
            root_dir = self._tempdir
        client = cdsapi.Client(url=self._configuration["cds_url"], key=cds_key)
        dataset = "seasonal-postprocessed-single-levels"
        variable = "total_precipitation_anomalous_rate_of_accumulation"

        for year in range(self._configuration["min_year"], today.year + 1):
            if year != today.year:
                months = [str(m) for m in range(1, 13)]
            else:
                months = [str(m) for m in range(1, today.month + 1)]
            file_name = f"{variable}_{year}.grib"
            filepath = join(root_dir, file_name)

            # check if data needs updating
            if year in existing_data:
                if year != today.year:
                    continue
                if dataset_month >= today.month:
                    continue
            request = {
                "originating_centre": "ecmwf",
                "system": "51",
                "variable": ["total_precipitation_anomalous_rate_of_accumulation"],
                "product_type": ["ensemble_mean"],
                "year": str(year),
                "month": months,
                "leadtime_month": ["1", "2", "3", "4", "5", "6"],
                "data_format": "grib",
            }
            if self._retriever.save and exists(filepath):
                self.grib_data.append(filepath)
                continue
            try:
                client.retrieve(dataset, request, filepath)
                self.grib_data.append(filepath)
            except HTTPError:
                continue
        if len(self.grib_data) > 0:
            return True
        return False

    def process(self) -> Dict:
        processed_data = {}
        for grib_data in self.grib_data:
            dataset = xr.open_mfdataset(
                grib_data,
                engine="cfgrib",
                drop_variables=["surface", "values"],
                backend_kwargs={"time_dims": ("forecastMonth", "time")},
            )
            dataset = dataset.assign_coords(
                longitude=(((dataset.longitude + 180) % 360) - 180)
            ).sortby("longitude")
            publish_dates = dataset.time.values
            forecast_months = dataset.forecastMonth.values
            for publish_date in publish_dates:
                for forecast_month in forecast_months:
                    data = dataset.sel(time=publish_date, forecastMonth=forecast_month)
                    date_str = datetime_as_string(publish_date, unit="M").replace(
                        "-", "_"
                    )
                    raster_name = f"anomalous_rate_of_accumulation_{date_str}_forecastmonth{forecast_month}.tif"
                    data.rio.to_raster(join(self._tempdir, raster_name))

        return processed_data

    def generate_dataset(self, processed_data: Dict) -> Optional[Dataset]:
        dataset_name = "ecmwf-anomalous-precipitation"
        dataset_title = "ECMWF SEA5 Seasonal Forecasts -- Anomalous Precipitation"
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
        dataset.add_other_location("world")

        # Add resources here

        return dataset


def _get_uploaded_data(force_refresh: bool) -> Tuple[List, int]:
    # TODO: rewrite this to download csv resources and get dates from there
    uploaded_data = []
    end_month = 0
    if force_refresh:
        return uploaded_data, end_month
    dataset = Dataset.read_from_hdx("ecmwf-anomalous-precipitation")
    if not dataset:
        return uploaded_data, end_month
    end_date = dataset.get_time_period()["enddate"]
    end_month = end_date.month
    resources = dataset.get_resources()
    for resource in resources:
        uploaded_data.append(resource["name"][-8:-4])
    return uploaded_data, end_month
