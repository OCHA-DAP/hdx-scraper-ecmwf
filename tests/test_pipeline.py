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
                configuration["min_year"] = 2025
                pipeline = Pipeline(configuration, retriever, tempdir)
                today = datetime(2025, 3, 15)
                updated = pipeline.download_cds_data(
                    cds_key="", today=today, force_refresh=False
                )
                assert updated is True

                pipeline.download_global_boundaries()
                pipeline.process()
                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "ecmwf-anomalous-precipitation",
                    "title": "ECMWF SEA5 Seasonal Forecasts - Anomalous Precipitation",
                    "dataset_date": "[2025-01-01T00:00:00 TO 2025-03-31T23:59:59]",
                    "tags": [
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "Placeholder",
                    "caveats": "Placeholder",
                    "dataset_source": "Climate Data Store",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "47677055-92e2-4f68-bf1b-5d570f27e791",
                    "data_update_frequency": 30,
                    "notes": "Placeholder",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "anomalous_precipitation_adm0.csv",
                        "description": "Summarized anomalous precipitation data at adm0 from 2025-01-01 to 2025-03-31",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "anomalous_precipitation_adm1.csv",
                        "description": "Summarized anomalous precipitation data at adm1 from 2025-01-01 to 2025-03-31",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "latest_anomalous_precipitation_geotiff.zip",
                        "description": "Latest anomalous precipitation data from 2025-03",
                        "format": "geotiff",
                    },
                ]

                assert_files_same(
                    join(fixtures_dir, "anomalous_precipitation_adm0.csv"),
                    join(tempdir, "anomalous_precipitation_adm0.csv"),
                )
                assert_files_same(
                    join(fixtures_dir, "anomalous_precipitation_adm1.csv"),
                    join(tempdir, "anomalous_precipitation_adm1.csv"),
                )
