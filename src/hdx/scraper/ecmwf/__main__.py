#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    temp_dir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.ecmwf._version import __version__
from hdx.scraper.ecmwf.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-ecmwf"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: ECMWF"
_FORCE_REFRESH = False


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    with temp_dir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)
            updated = pipeline.download_cds_data(
                cds_key=getenv("CDS_KEY"), today=now_utc(), force_refresh=_FORCE_REFRESH
            )
            if not updated:
                logger.info("Data has not been updated")
                return

            pipeline.download_global_boundaries()
            pipeline.process()
            dataset = pipeline.generate_dataset()
            dataset.update_from_yaml(
                script_dir_plus_file(join("config", "hdx_dataset_static.yaml"), main)
            )
            dataset.create_in_hdx(
                remove_additional_resources=True,
                match_resource_order=False,
                hxl_update=False,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )
            logger.info("Finished processing!")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
