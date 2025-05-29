
from dagster import asset, get_dagster_logger, MetadataValue, Output, multi_asset, AssetOut
import pandas as pd
from sklearn.model_selection import train_test_split


@multi_asset(
    group_name="preprocessing",
    deps=["combined_data"],
    outs={
        "train_data": AssetOut(io_manager_key="file_io_manager"),
        "test_data": AssetOut(io_manager_key="file_io_manager"),
        "split_metadata": AssetOut(io_manager_key="file_io_manager")
    },
    compute_kind="pandas",
)
def train_test_data(context, combined_data):
    """Split the combined data into train and test sets with metadata"""
    logger = get_dagster_logger()
    
    # Log incoming data info
    logger.info(f"Splitting combined data: {combined_data.shape}")
    
    # Define features and target
    # Assuming 'popularity' is your target column - adjust if different
    if 'popularity' not in combined_data.columns:
        raise ValueError("Expected 'popularity' column not found in dataset")
    
    combined_data_clean = combined_data.drop(columns=['Unnamed: 0'], errors='ignore')

    # Get features and target
    X = combined_data_clean.drop('popularity', axis=1)
    y = combined_data_clean['popularity']
    y = (y > 50).astype(int)
    

    # Split data (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if len(y.unique()) < 10 else None
    )
    
    # Recombine features and target for easier handling
    train_data = X_train.copy()
    train_data['popularity'] = y_train
    
    test_data = X_test.copy()
    test_data['popularity'] = y_test
    
    # Create split metadata
    split_metadata = {
        "train_size": len(train_data),
        "test_size": len(test_data),
        "train_percentage": len(train_data) / len(combined_data_clean) * 100,
        "test_percentage": len(test_data) / len(combined_data_clean) * 100,
        "target_column": "popularity",
        "target_distribution_train": y_train.value_counts(normalize=True).to_dict(),
        "target_distribution_test": y_test.value_counts(normalize=True).to_dict(),
        "feature_columns": X_train.columns.tolist()
    }
    
    # Log split information
    logger.info(f"Train data shape: {train_data.shape}")
    logger.info(f"Test data shape: {test_data.shape}")
    logger.info(f"Split ratio: {split_metadata['train_percentage']:.1f}% train, {split_metadata['test_percentage']:.1f}% test")
    
     # Add metadata to context
    context.add_output_metadata(
        {
            "rows": MetadataValue.int(int(len(train_data))),
            "columns": MetadataValue.int(int(len(train_data.columns))),
            "memory_usage_mb": MetadataValue.float(float(train_data.memory_usage(deep=True).sum() / 1e6))
        },
        output_name="train_data" 
    )
    
    # Fix this line - it was missing the int() function call
    context.add_output_metadata(
        {
            "rows": MetadataValue.int(int(len(test_data))),  # FIXED - was incomplete
            "columns": MetadataValue.int(int(len(test_data.columns))),
            "memory_usage_mb": MetadataValue.float(float(test_data.memory_usage(deep=True).sum() / 1e6))
        },
        output_name="test_data"
    )
    
    context.add_output_metadata(
        {
            "train_size": MetadataValue.int(int(split_metadata["train_size"])),
            "test_size": MetadataValue.int(int(split_metadata["test_size"])),
            "train_percentage": MetadataValue.float(float(split_metadata["train_percentage"])),
            "feature_count": MetadataValue.int(int(len(split_metadata["feature_columns"])))
        },
        output_name="split_metadata" 
    )
    
    
    # Return the three outputs in the order defined in the outs
    return (train_data, test_data, split_metadata)


