#!/usr/bin/python
"""ECMWF scraper"""

import logging
from calendar import monthrange
from datetime import datetime
from os.path import basename, exists, join
from typing import Dict, Optional, Tuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
import xarray as xr
from cdsapi import Client
from dateutil.relativedelta import relativedelta
from exactextract import exact_extract
from geopandas import read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
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
        self.processed_data = {}
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
        return

    def download_cds_data(
        self, cds_key: str, today: datetime, force_refresh: bool = False
    ) -> bool:
        self._get_uploaded_data(today, force_refresh)
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
                _ = self.download_grib(client, request, dataset, filepath)

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

    def process(self, today: datetime) -> None:
        regions = _get_region_info()
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
            dataset = dataset.rio.write_crs("EPSG:4326")
            issue_dates = dataset.time.values
            if not isinstance(issue_dates, np.ndarray):
                issue_dates = np.asarray(issue_dates)
            leadtime_months = dataset.forecastMonth.values

            # save to raster
            for issue_date in issue_dates:
                logger.info(f"Processing issue date: {issue_dates}")
                year = np.datetime_as_string(issue_date, unit="Y")
                month = np.datetime_as_string(issue_date, unit="M")[-2:]
                for leadtime_month in leadtime_months:
                    # convert to accumulation
                    valid_time = pd.to_datetime(issue_date) + relativedelta(
                        months=leadtime_month - 1
                    )
                    numdays = monthrange(valid_time.year, valid_time.month)[1]
                    data = dataset.sel(time=issue_date, forecastMonth=leadtime_month)
                    data = data * numdays * 24 * 60 * 60 * 1000
                    raster_name = f"anomalous_accumulation_{year}_{month}_leadtime{int(leadtime_month) - 1}.tif"
                    out_tif = join(self._tempdir, raster_name)
                    data.rio.to_raster(out_tif)
                    self.raster_data.append(out_tif)

                    # calculate statistics
                    for admin_level in ["0", "1"]:
                        adm_data = self.global_boundaries[admin_level]
                        include_cols = [
                            f"adm{level}_pcode"
                            for level in range(0, int(admin_level) + 1)
                        ] + [
                            f"adm{level}_name"
                            for level in range(0, int(admin_level) + 1)
                        ]
                        results_zs = exact_extract(
                            out_tif,
                            adm_data,
                            ["count", "mean", "median"],
                            include_cols=include_cols,
                            output="pandas",
                        )
                        results_zs.rename(
                            columns={
                                "count": "pixel_count",
                                "mean": "mean_anomaly",
                                "median": "median_anomaly",
                            },
                            inplace=True,
                        )

                        # add admin 0 fields
                        iso_codes = {}
                        country_names = {}
                        country_codes = list(set(results_zs["adm0_pcode"]))
                        for country_code in country_codes:
                            iso_code, country_name = _get_country_info(country_code)
                            iso_codes[country_code] = iso_code
                            country_names[country_code] = country_name
                        results_zs["iso_code"] = results_zs["adm0_pcode"].map(iso_codes)
                        results_zs["adm0_name"] = results_zs["adm0_pcode"].map(
                            country_names
                        )
                        results_zs.drop("adm0_pcode", axis=1, inplace=True)

                        # add other needed fields
                        results_zs["admin_level"] = admin_level
                        results_zs["issue_year"] = int(year)
                        results_zs["issue_month"] = int(month)
                        results_zs["lead_time"] = int(leadtime_month) - 1
                        results_zs["valid_year"] = int(valid_time.year)
                        results_zs["valid_month"] = int(valid_time.month)

                        # add to processed data dataframes
                        if admin_level == "0":
                            identifier = "adm0"
                            self._add_processed_rows(identifier, results_zs)
                        else:
                            past_3yrs = today - relativedelta(years=3)
                            if valid_time >= past_3yrs:
                                identifier = "adm1_global_3yrs"
                                self._add_processed_rows(identifier, results_zs)
                            for region_name, iso_list in regions.items():
                                identifier = f"adm1_{region_name.lower()}"
                                results_subset = results_zs[
                                    results_zs["iso_code"].isin(iso_list)
                                ]
                                if len(results_subset) == 0:
                                    continue
                                self._add_processed_rows(identifier, results_subset)

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
                self.processed_data["adm0"]["issue_year"],
                self.processed_data["adm0"]["issue_month"],
            )
        ]
        start_date = parse_date(f"{min(dates)}-01")
        end_date = parse_date(f"{max(dates)}-01")
        end_date = end_date + relativedelta(day=31)
        dataset.set_time_period(startdate=start_date, enddate=end_date)

        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")

        # Add csv resources
        for identifier in self.processed_data:
            admin_level = identifier[3]
            fields = ["iso_code", "adm0_name"]
            if admin_level == "1":
                fields = fields + ["adm1_pcode", "adm1_name"]
            fields = fields + [
                "admin_level",
                "issue_year",
                "issue_month",
                "lead_time",
                "valid_year",
                "valid_month",
            ]
            processed_data = self.processed_data[identifier][
                fields + ["pixel_count", "mean_anomaly", "median_anomaly"]
            ]
            processed_data.sort_values(by=fields, inplace=True)

            filename = f"forecast_precipitation_anomalies_{identifier}.csv"
            description = f"Summarized forecast precipitation anomalies data at adm{admin_level} from {iso_string_from_datetime(start_date)} to {iso_string_from_datetime(end_date)}"
            if admin_level == "1" and "global" not in identifier:
                region = identifier.split("_")[1]
                description += f" for {region.title()}"
            resourcedata = {
                "name": filename,
                "description": description,
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
            "_".join(basename(raster).split("_")[2:4]) for raster in self.raster_data
        ]
        latest_date = max(raster_dates)
        raster_paths = [
            raster for raster in self.raster_data if latest_date in basename(raster)
        ]
        latest_zip = join(
            self._tempdir, f"forecast_precipitation_anomalies_geotiff_{latest_date}.zip"
        )
        with ZipFile(latest_zip, "w") as z:
            for raster_path in raster_paths:
                z.write(raster_path, basename(raster_path))
        resource = Resource(
            {
                "name": basename(latest_zip),
                "description": f"Latest forecast precipitation anomalies raster data from {latest_date.replace('_', '-')}",
            }
        )
        resource.set_format("GeoTIFF")
        resource.set_file_to_upload(latest_zip)
        dataset.add_update_resource(resource)

        return dataset

    def _get_uploaded_data(self, today: datetime, force_refresh: bool) -> None:
        if force_refresh:
            return
        dataset = Dataset.read_from_hdx("ecmwf-anomalous-precipitation")
        if not dataset:
            return
        resources = dataset.get_resources()
        for resource in resources:
            if resource.get_format() != "csv":
                continue
            file_path = self._retriever.download_file(resource["url"])
            identifier = "_".join(resource["name"][:-4].split("_")[3:])
            uploaded_data = pd.read_csv(file_path)
            dates = [
                str(y) + "-" + str(m).zfill(2)
                for y, m in zip(
                    uploaded_data["issue_year"], uploaded_data["issue_month"]
                )
            ]
            # filter data to only include past 3 years
            if "3yrs" in resource["name"]:
                past_3yrs = today - relativedelta(years=3)
                past_3yrs = f"{past_3yrs.year}-{past_3yrs.month}"
                uploaded_data = uploaded_data[[d > past_3yrs for d in dates]]
            self.processed_data[identifier] = uploaded_data

            dates = list(set(dates + self.existing_dates))
            self.existing_dates = sorted(dates)

    def _add_processed_rows(self, identifier: str, df: pd.DataFrame()) -> None:
        if self.processed_data.get(identifier) is None:
            self.processed_data[identifier] = df
        else:
            self.processed_data[identifier] = pd.concat(
                [
                    self.processed_data[identifier],
                    df,
                ],
                ignore_index=True,
            )


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


def _get_region_info() -> Dict[str, str]:
    region_info = {}
    country_info = Country.countriesdata()["countries"]
    for iso, country in country_info.items():
        region = country["#region+main+name+preferred"]
        if not region:
            continue
        dict_of_lists_add(region_info, region, iso)
    return region_info
