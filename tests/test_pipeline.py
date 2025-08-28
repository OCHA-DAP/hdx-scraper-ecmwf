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
                updated = pipeline.download_rasters(
                    cds_key="", today=today, force_refresh=False
                )
                assert updated is True

                pipeline.download_global_boundaries()
                pipeline.process()
                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {}

                resources = dataset.get_resources()
                assert resources == [{}]

                assert_files_same(
                    join(fixtures_dir, ""),
                    join(tempdir, ""),
                )
