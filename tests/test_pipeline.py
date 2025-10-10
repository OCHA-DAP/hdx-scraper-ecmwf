from datetime import datetime
from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.ecmwf.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "TestECMWF",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                configuration["min_year"] = 2024
                pipeline = Pipeline(configuration, retriever, tempdir)
                today = datetime(2025, 3, 15)
                updated = pipeline.download_cds_data(
                    cds_key="", today=today, force_refresh=False
                )
                assert updated is True

                pipeline.download_global_boundaries()
                pipeline.process(today)
                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "ecmwf-anomalous-precipitation",
                    "title": "ECMWF SEA5 Seasonal Forecasts - Anomalous Precipitation",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2025-03-31T23:59:59]",
                    "tags": [
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "forecasting",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "Anomalies are calculated by subtracting the climatology (long term average from 1993-2016) from each individual forecast.\n\n"
                    "Data is converted from a monthly anomalous precipitation rate to total accumulated precipitation per month in mm.\n\n"
                    "See an example of these calculations [here](https://ecmwf-projects.github.io/copernicus-training-c3s/sf-anomalies.html).\n\n"
                    "The anomalies are made available in a tabular format by taking the mean and median of all grid cells within administrative boundary polygons at the admin 0 and admin 1 levels.\n",
                    "caveats": "The gridded product from ECMWF is provided at a 1 degree resolution.\n\n"
                    "Forecast skill is variable, particularly across longer lead times. Forecast skill may also vary significantly by location, with even a negative correlation in some locations and lead times. See [these plots](https://confluence.ecmwf.int/display/CKB/C3S+seasonal+forecasts+verification+plots) from ECMWF for more information.\n\n"
                    "Users should be careful in their handling of the temporal attributes of this dataset. Variables prefixed with “issue_” refer to the time that the forecast was issued by ECMWF. Variables prefixed with “valid_” refer to the time that the forecast applies to. The difference in months between “issued_” and “valid_” dates is captured by the “lead_time” variable.\n",
                    "dataset_source": "Copernicus Climate Data Store",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "hdx",
                    "data_update_frequency": 30,
                    "notes": "This data can be used to identify how forecasted precipitation may differ from the long term average, for a given location and time of the year. These anomalies are calculated by the European Centre for Medium-Range Weather Forecasts (ECMWF) and are based on their SEAS5 seasonal precipitation forecast. SEAS5 is an ensemble forecast, meaning that the weather model outputs many possible scenarios, leading to probabilistic outcomes. The data presented here is the result of averaging all outputs (the “ensemble mean”). Additional postprocessing has been applied here to aggregate ECMWF’s gridded outputs across administrative boundaries (at both admin 0 and admin 1 levels). The gridded product can also be accessed from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/datasets/seasonal-postprocessed-single-levels?tab=overview).\n\n"
                    "Data is available on the 5th of each month, with up to 6 months of lead time (including the current month). Anomaly values are in mm/month, with positive values indicating above average precipitation. The resources in this dataset include:\n\n"
                    "  - Full admin 0 historical record of anomalies (forecast_precipitation_anomalies_adm0.csv)\n\n"
                    "  - Recent admin 1 global anomalies (forecast_precipitation_anomalies_adm1_global_3yrs.csv)\n\n"
                    "  - Full admin 1 historical record of anomalies, separated by region (forecast_precipitation_anomalies_adm1_{region}.csv)\n\n"
                    "  - Geotiffs of anomalies based on latest forecast (forecast_precipitation_anomalies_geotiff_{forecast_date}.zip)\n",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "forecast_precipitation_anomalies_adm0.csv",
                        "description": "Summarized forecast precipitation anomalies data at adm0 from 2024-01-01 to 2025-03-31",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "forecast_precipitation_anomalies_adm1_global_3yrs.csv",
                        "description": "Summarized forecast precipitation anomalies data at adm1 from 2024-01-01 to 2025-03-31",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "forecast_precipitation_anomalies_adm1_asia.csv",
                        "description": "Summarized forecast precipitation anomalies data at adm1 from 2024-01-01 to 2025-03-31 for Asia",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "forecast_precipitation_anomalies_geotiff_2025_03.zip",
                        "description": "Latest forecast precipitation anomalies raster data from 2025-03",
                        "format": "geotiff",
                    },
                ]

                assert_files_same(
                    join(fixtures_dir, "forecast_precipitation_anomalies_adm0.csv"),
                    join(tempdir, "forecast_precipitation_anomalies_adm0.csv"),
                )
                assert_files_same(
                    join(
                        fixtures_dir,
                        "forecast_precipitation_anomalies_adm1_global_3yrs.csv",
                    ),
                    join(
                        tempdir, "forecast_precipitation_anomalies_adm1_global_3yrs.csv"
                    ),
                )
                assert_files_same(
                    join(
                        fixtures_dir, "forecast_precipitation_anomalies_adm1_asia.csv"
                    ),
                    join(tempdir, "forecast_precipitation_anomalies_adm1_asia.csv"),
                )
