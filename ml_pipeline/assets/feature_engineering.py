from dagster import asset, get_dagster_logger, MetadataValue, AssetIn, Config, AssetOut, multi_asset
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import pickle
import io

# Import your preprocessing classes - adjust paths as needed
from ml_pipeline.preprocessing.clean_data import DataCleaner
from ml_pipeline.preprocessing.artist_popularity import ArtistPopularityEncoder
from ml_pipeline.preprocessing.string_toInt_encoder import SafeOneHotEncoder



@multi_asset(
    group_name="preprocessing",
    ins={
        "test_data": AssetIn(key="test_data"),
        "train_data": AssetIn(key="train_data")
    },
    outs={
        "preprocessor": AssetOut(io_manager_key="file_io_manager"),
        "X_train_processed": AssetOut(io_manager_key="file_io_manager"),
        "y_train": AssetOut(io_manager_key="file_io_manager"),
        "X_test_processed": AssetOut(io_manager_key="file_io_manager"),
        "y_test": AssetOut(io_manager_key="file_io_manager")
    },
    compute_kind="sklearn",
)
def base_preprocessor(context, train_data, test_data):
    """Create the base preprocessing pipeline"""

    logger = get_dagster_logger()

    X_train = train_data.drop(columns=['popularity'], errors='ignore')
    y_train = train_data['popularity']

    X_test = test_data.drop(columns=['popularity'], errors='ignore')
    y_test = test_data['popularity']

    preprocessor = Pipeline([
        ('artist_encoder', ArtistPopularityEncoder()),
        ('data_cleaner', DataCleaner()),
        ('encoder', SafeOneHotEncoder()),
        ('scaler', StandardScaler()),
    ])
    
    # Fit the preprocessor on training data
    logger.info(f"Fitting preprocessor on training data...")
    preprocessor.fit(X_train, y_train)
    
    # Transform both datasets
    logger.info("Transforming train and test data...")
    X_train_processed = preprocessor.transform(X_train)
    X_test_processed = preprocessor.transform(X_test)

    if isinstance(X_train_processed, np.ndarray):
        try:
            feature_names = preprocessor.get_feature_names_out()
        except:
            feature_names = [f"feature_{i}" for i in range(X_train_processed.shape[1])]
        
        X_train_processed = pd.DataFrame(X_train_processed, columns=feature_names)
        X_test_processed = pd.DataFrame(X_test_processed, columns=feature_names)
    
    # Log shapes of processed data
    logger.info(f"Processed train data shape: {X_train_processed.shape}")
    logger.info(f"Processed test data shape: {X_test_processed.shape}")
    
    # Add metadata for each output
    context.add_output_metadata({
        "preprocessing_steps": MetadataValue.json([step[0] for step in preprocessor.steps]),
        "input_train_shape": MetadataValue.text(str(X_train.shape)),
        "input_test_shape": MetadataValue.text(str(X_test.shape)),
        "output_train_shape": MetadataValue.text(str(X_train_processed.shape)),
        "output_test_shape": MetadataValue.text(str(X_test_processed.shape)),
        "feature_count": MetadataValue.int(X_train_processed.shape[1])
    }, output_name="preprocessor")
    
    context.add_output_metadata({
        "rows": MetadataValue.int(int(len(X_train_processed))),
        "columns": MetadataValue.int(int(len(X_train_processed.columns))),
        "memory_usage_mb": MetadataValue.float(float(X_train_processed.memory_usage(deep=True).sum() / 1e6))
    }, output_name="X_train_processed")
    
    context.add_output_metadata({
        "rows": MetadataValue.int(int(len(X_test_processed))),
        "columns": MetadataValue.int(int(len(X_test_processed.columns))),
        "memory_usage_mb": MetadataValue.float(float(X_test_processed.memory_usage(deep=True).sum() / 1e6))
    }, output_name="X_test_processed")
    
    # Return outputs in the order defined in outs
    return (preprocessor, X_train_processed, y_train, X_test_processed, y_test)