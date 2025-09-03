#!/usr/bin/python
"""ECMWF scraper"""

import logging
from datetime import datetime
from os.path import basename, exists, join
from typing import Optional, Tuple
from zipfile import ZipFile

import pandas as pd
import xarray as xr
from cdsapi import Client
from dateutil.relativedelta import relativedelta
from geopandas import read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from numpy import asarray, datetime_as_string, ndarray
from pandas import DataFrame
from rasterstats import zonal_stats
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.global_boundaries = {}
        self.grib_data = []
        self.existing_dates = []
        self.processed_data = {
            "adm0": DataFrame(),
            "adm1": DataFrame(),
        }
        self.raster_data = []

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

    def download_cds_data(
        self, cds_key: str, today: datetime, force_refresh: bool = False
    ) -> bool:
        self._get_uploaded_data(force_refresh)
        if self._retriever.save or self._retriever.use_saved:
            root_dir = self._retriever.saved_dir
        else:
            root_dir = self._tempdir
        client = Client(url=self._configuration["cds_url"], key=cds_key)
        dataset = "seasonal-postprocessed-single-levels"
        variable = "total_precipitation_anomalous_rate_of_accumulation"

        for year in range(self._configuration["min_year"], today.year + 1):
            # create list of missing data that needs to be added
            months = []
            end_month = 12 if year != today.year else today.month
            for month in range(1, end_month + 1):
                data_date = f"{year}-{str(month).zfill(2)}"
                if data_date not in self.existing_dates:
                    months.append(str(month))
            if len(months) == 0:
                continue

            file_name = f"{variable}_{year}.grib"
            filepath = join(root_dir, file_name)
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
            success = self.download_grib(client, request, dataset, filepath)
            if year == today.year and not success:
                logger.info("Download failed, trying without current month")
                request = {
                    "originating_centre": "ecmwf",
                    "system": "51",
                    "variable": ["total_precipitation_anomalous_rate_of_accumulation"],
                    "product_type": ["ensemble_mean"],
                    "year": str(year),
                    "month": months[:-1],
                    "leadtime_month": ["1", "2", "3", "4", "5", "6"],
                    "data_format": "grib",
                }
                success = self.download_grib(client, request, dataset, filepath)

        if len(self.grib_data) > 0:
            return True
        return False

    def download_grib(
        self, client: Client, request: dict, dataset: str, filepath: str
    ) -> bool:
        if exists(filepath):
            self.grib_data.append(filepath)
            return True
        try:
            client.retrieve(dataset, request, filepath)
            self.grib_data.append(filepath)
        except HTTPError:
            return False
        return True

    def process(self) -> None:
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
            if not isinstance(publish_dates, ndarray):
                publish_dates = asarray(publish_dates)
            forecast_months = dataset.forecastMonth.values

            # save to raster
            for publish_date in publish_dates:
                processed_data = {
                    "adm0": DataFrame(),
                    "adm1": DataFrame(),
                }
                year = datetime_as_string(publish_date, unit="Y")
                month = datetime_as_string(publish_date, unit="M")[-2:]
                for forecast_month in forecast_months:
                    data = dataset.sel(time=publish_date, forecastMonth=forecast_month)
                    raster_name = f"anomalous_rate_of_accumulation_{year}_{month}_forecastmonth{forecast_month}.tif"
                    out_tif = join(self._tempdir, raster_name)
                    data.rio.to_raster(out_tif)
                    self.raster_data.append(out_tif)

                    # calculate statistics
                    for admin_level in ["0", "1"]:
                        adm_data = self.global_boundaries[admin_level]
                        results_zs = zonal_stats(
                            vectors=adm_data[["geometry"]],
                            raster=out_tif,
                            all_touched=False,
                            stats=["mean", "median"],
                        )
                        fields = (
                            ["mean", "median"]
                            + [
                                f"adm{level}_pcode"
                                for level in range(0, int(admin_level) + 1)
                            ]
                            + [
                                f"adm{level}_name"
                                for level in range(0, int(admin_level) + 1)
                            ]
                        )
                        results_zs = pd.DataFrame.from_dict(results_zs).join(adm_data)[
                            fields
                        ]
                        results_zs = results_zs.rename(
                            columns={
                                "mean": f"mean_forecast{forecast_month}",
                                "median": f"median_forecast{forecast_month}",
                            }
                        )

                        # add to processed data dataframe
                        if len(processed_data[f"adm{admin_level}"]) == 0:
                            processed_data[f"adm{admin_level}"] = results_zs
                        else:
                            pcode_fields = [
                                f"adm{level}_pcode"
                                for level in range(0, int(admin_level) + 1)
                            ]
                            subset_fields = [
                                f"mean_forecast{forecast_month}",
                                f"median_forecast{forecast_month}",
                            ] + pcode_fields
                            processed_data[f"adm{admin_level}"] = pd.merge(
                                processed_data[f"adm{admin_level}"],
                                results_zs[subset_fields],
                                how="outer",
                                on=pcode_fields,
                            )

                for admin_level in ["0", "1"]:
                    processed_data_adm = processed_data[f"adm{admin_level}"]
                    # add admin 0 fields
                    iso_codes = {}
                    country_names = {}
                    country_codes = list(set(processed_data_adm["adm0_pcode"]))
                    for country_code in country_codes:
                        iso_code, country_name = _get_country_info(country_code)
                        iso_codes[country_code] = iso_code
                        country_names[country_code] = country_name
                    processed_data_adm["iso_code"] = processed_data_adm[
                        "adm0_pcode"
                    ].map(iso_codes)
                    processed_data_adm["adm0_name"] = processed_data_adm[
                        "adm0_pcode"
                    ].map(country_names)
                    processed_data_adm.drop("adm0_pcode", axis=1, inplace=True)

                    # add other needed fields
                    processed_data_adm["admin_level"] = admin_level
                    processed_data_adm["publish_year"] = int(year)
                    processed_data_adm["publish_month"] = int(month)

                    if len(self.processed_data[f"adm{admin_level}"]) == 0:
                        self.processed_data[f"adm{admin_level}"] = processed_data_adm
                    else:
                        self.processed_data[f"adm{admin_level}"] = pd.concat(
                            [
                                self.processed_data[f"adm{admin_level}"],
                                processed_data_adm,
                            ],
                            ignore_index=True,
                        )
        return

    def generate_dataset(self) -> Optional[Dataset]:
        dataset_name = "ecmwf-anomalous-precipitation"
        dataset_title = "ECMWF SEA5 Seasonal Forecasts - Anomalous Precipitation"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dates = [
            str(y) + "-" + str(m).zfill(2)
            for y, m in zip(
                self.processed_data["adm0"]["publish_year"],
                self.processed_data["adm0"]["publish_month"],
            )
        ]
        start_date = parse_date(f"{min(dates)}-01")
        end_date = parse_date(f"{max(dates)}-01")
        end_date = end_date + relativedelta(day=31)
        dataset.set_time_period(startdate=start_date, enddate=end_date)

        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")

        # Add csv resources
        for admin_level in ["0", "1"]:
            subset_fields = ["iso_code", "adm0_name"]
            sort_fields = ["iso_code"]
            if admin_level == "1":
                subset_fields = subset_fields + ["adm1_pcode", "adm1_name"]
                sort_fields.append("adm1_pcode")
            subset_fields = subset_fields + [
                "admin_level",
                "publish_year",
                "publish_month",
            ]
            sort_fields = sort_fields + ["publish_year", "publish_month"]
            for forecast in range(1, 7):
                subset_fields = subset_fields + [
                    f"mean_forecast{forecast}",
                    f"median_forecast{forecast}",
                ]
            processed_data = self.processed_data[f"adm{admin_level}"][subset_fields]
            processed_data.sort_values(by=sort_fields, inplace=True)

            filename = f"anomalous_precipitation_adm{admin_level}.csv"
            resourcedata = {
                "name": filename,
                "description": "",
                "p_coded": True,
            }
            dataset.generate_resource_from_iterable(
                headers=list(processed_data.columns),
                iterable=processed_data.to_dict(orient="records"),
                hxltags={},
                folder=self._tempdir,
                filename=filename,
                resourcedata=resourcedata,
                encoding="utf-8-sig",
            )

        # Add zipped raster resource
        raster_dates = [
            "_".join(basename(raster).split("_")[4:6]) for raster in self.raster_data
        ]
        latest_date = max(raster_dates)
        raster_paths = [
            raster for raster in self.raster_data if latest_date in basename(raster)
        ]
        latest_zip = join(self._tempdir, "latest_anomalous_precipitation_geotiff.zip")
        with ZipFile(latest_zip, "w") as z:
            for raster_path in raster_paths:
                z.write(raster_path, basename(raster_path))
        resource = Resource(
            {
                "name": basename(latest_zip),
                "description": f"Latest anomalous precipitation data from {latest_date.replace('_', '-')}",
            }
        )
        resource.set_format("GeoTIFF")
        resource.set_file_to_upload(latest_zip)
        dataset.add_update_resource(resource)

        return dataset

    def _get_uploaded_data(self, force_refresh: bool) -> None:
        if force_refresh:
            return
        dataset = Dataset.read_from_hdx("ecmwf-anomalous-precipitation")
        if not dataset:
            return
        dates = []
        resources = dataset.get_resources()
        for resource in resources:
            if resource.get_format() != "csv":
                continue
            file_path = self._retriever.download_file(resource["url"])
            admin_level = resource["name"][-8:-4]
            uploaded_data = pd.read_csv(file_path)
            self.processed_data[admin_level] = uploaded_data
            if len(dates) == 0:
                dates = [
                    str(y) + "-" + str(m).zfill(2)
                    for y, m in zip(
                        uploaded_data["publish_year"], uploaded_data["publish_month"]
                    )
                ]
                dates = list(set(dates))
                self.existing_dates = sorted(dates)


def _get_country_info(country_code: str) -> Tuple[str, str]:
    country_name = None
    iso_code = None
    if len(country_code) > 3:
        country_code = country_code[:2]
    if len(country_code) == 3:
        iso_code = country_code
    if len(country_code) == 2:
        iso_code = Country.get_iso3_from_iso2(country_code)
    if iso_code:
        country_name = Country.get_country_name_from_iso3(iso_code)
    else:
        logger.error(f"Unknown country code: {country_code}")
    return iso_code, country_name
