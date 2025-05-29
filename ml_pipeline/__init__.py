from dagster import Definitions
from dagster import repository, job, asset, AssetIn

from ml_pipeline.assets.data_ingestion import raw_kaggle_data, spotify_data_analysis, yearly_data
# from ml_pipeline.assets.data_ingestion import processed_spotify_data

from ml_pipeline.resources.mlflow_resource import MLflowTrackingResource
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs_client_resource import lakefs_client_resource
from ml_pipeline.resources.lakefs_spec_resource import lakefs_fs_resource
from ml_pipeline.io_manager.lakefs_io import dynamic_lakefs_io_manager
from ml_pipeline.io_manager.memory_io_manager import custom_memory_io
from ml_pipeline.io_manager.file_io_manager import file_io_manager
from ml_pipeline.assets.data_selection import selected_years, combined_data
from ml_pipeline.assets.split_train_test import train_test_data
from ml_pipeline.resources.year_selector_resource import year_selector
from ml_pipeline.assets.feature_engineering import base_preprocessor

# from ml_pipeline.assets.model_training import random_forest_train, random_forest_test 
from ml_pipeline.assets.model_training import enhanced_debug_mlflow, xgboost_train, xgboost_test
# from ml_pipeline.assets.model_training import gradient_boosting_train, gradient_boosting_test 
# from ml_pipeline.assets.model_training import ridge_train, ridge_test


defs = Definitions(
    assets=[
        raw_kaggle_data,
        spotify_data_analysis,
        yearly_data,
        selected_years,
        combined_data,
        train_test_data,
        base_preprocessor,
        
        # random_forest_train,
        # random_forest_test,
        xgboost_train,
        xgboost_test,
        enhanced_debug_mlflow,
        # gradient_boosting_train,
        # gradient_boosting_test,
        # ridge_train,
        # ridge_test,
    ],
    resources={
        "kaggle": kaggle_api,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource,
        "custom_memory_io": custom_memory_io,
        "file_io_manager": file_io_manager,
        "dynamic_lakefs_io": dynamic_lakefs_io_manager.configured({
            "default_repo": "spotify-repo",
            "default_branch": "main"
        }),
        "year_selector": year_selector,
         "mlflow_tracking": MLflowTrackingResource(  # ADD THIS BLOCK
            tracking_uri="http://mlflow:5000",
            base_experiment_name="spotify_popularity_prediction",
            enable_autolog=True,
        )
        
    },
)


