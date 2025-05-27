from dagster import Definitions
from dagster import repository, job, asset, AssetIn

from ml_pipeline.assets.data_ingestion import raw_kaggle_data, spotify_data_analysis, yearly_data
# from ml_pipeline.assets.data_ingestion import processed_spotify_data

from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs_client_resource import lakefs_client_resource
from ml_pipeline.resources.lakefs_spec_resource import lakefs_fs_resource
from ml_pipeline.io_manager.lakefs_io import dynamic_lakefs_io_manager
from ml_pipeline.assets.data_selection import selected_years, combined_data
from ml_pipeline.resources.year_selector_resource import year_selector


defs = Definitions(
    assets=[
        raw_kaggle_data,
        spotify_data_analysis,
        yearly_data,
        selected_years,
        combined_data,
    ],
    resources={
        "kaggle": kaggle_api,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource,
        "dynamic_lakefs_io": dynamic_lakefs_io_manager.configured({
            "default_repo": "spotify-repo",
            "default_branch": "main"
        }),
        "year_selector": year_selector
    },
)


