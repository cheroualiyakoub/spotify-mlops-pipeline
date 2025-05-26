from dagster import Definitions
from dagster import repository, job, asset, AssetIn

from ml_pipeline.assets.data_ingestion import versioned_spotify_data_dev, spotify_data_analysis, yearly_data
# from ml_pipeline.assets.data_ingestion import processed_spotify_data

from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs_client_resource import  lakefs_client_resource
from ml_pipeline.resources.lakefs_spec_resource import  lakefs_fs_resource
from ml_pipeline.io_manager.lakefs_io import  dynamic_lakefs_io_manager
from ml_pipeline.assets.feature_engineering import historical_training_data, available_years, \
                    combined_historical_data, latest_combined_data_by_push_date


# defs = Definitions(
#     assets=[versioned_spotify_data, processed_spotify_data],
#     resources={
#         "kaggle": kaggle_api,
#         "lakefs_client": lakefs_client_resource,
#         "lakefs_fs": lakefs_fs_resource,
#          "lakefs_io": lakefs_io_manager
#     }
# )
# In __init__.py



defs = Definitions(
    assets=[
        versioned_spotify_data_dev,
        spotify_data_analysis,
        yearly_data,
        historical_training_data,
        available_years,
        combined_historical_data,
        latest_combined_data_by_push_date
    ],
    resources={
        "kaggle": kaggle_api,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource,
        "dynamic_lakefs_io": dynamic_lakefs_io_manager.configured({
            "default_repo": "spotify-repo",
            "default_branch": "main"
        })
    },
    # io_managers={

    # }
)
