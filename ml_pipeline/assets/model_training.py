from dagster import asset, AssetIn, Output, MetadataValue
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import pickle

# Factory function to create model training assets
def create_model_asset(model_name, model_class, model_params):
    """
    Factory function to create separate training and testing assets for a model
    
    Returns a tuple of (training_asset, testing_asset)
    """
    
    # Training Asset
    @asset(
        name=f"{model_name}_trained",
        group_name="models",
        ins={
            "X_train_processed": AssetIn(key="X_train_processed"),
            "y_train": AssetIn(key="y_train")
        },
        io_manager_key="file_io_manager",
    )
    def train_model_asset(context, X_train_processed, y_train):
        """Train a model on processed training data."""
        logger = context.log
        
        logger.info(f"Training {model_name} model...")
        logger.info(f"Training data shape: {X_train_processed.shape}")
        logger.info(f"Target shape: {y_train.shape}")
        
        # Initialize and train the model
        model = model_class(**model_params)
        model.fit(X_train_processed, y_train)
        
        # Make predictions on training data for training metrics
        train_preds = model.predict(X_train_processed)
        
        # Calculate training metrics - Convert numpy types to Python native types
        train_metrics = {
            "train_rmse": float(np.sqrt(mean_squared_error(y_train, train_preds))),
            "train_r2": float(r2_score(y_train, train_preds)),
            "train_mae": float(mean_absolute_error(y_train, train_preds)),
        }
        
        logger.info(f"Training metrics for {model_name}: {train_metrics}")
        
        # Add metadata
        context.add_output_metadata({
            "model_type": MetadataValue.text(model_name),
            "model_params": MetadataValue.json(model_params),
            "train_rmse": MetadataValue.float(train_metrics["train_rmse"]),
            "train_r2": MetadataValue.float(train_metrics["train_r2"]),
            "train_mae": MetadataValue.float(train_metrics["train_mae"]),
            "training_samples": MetadataValue.int(len(X_train_processed)),
            "features_count": MetadataValue.int(X_train_processed.shape[1])
        })
        
        # Return trained model with training metrics
        return {
            "model": model,
            "train_metrics": train_metrics,
            "model_params": model_params,
            "model_type": model_name
        }
    
    # Testing Asset
    @asset(
        name=f"{model_name}_evaluated",
        group_name="models",
        ins={
            f"{model_name}_trained": AssetIn(key=f"{model_name}_trained"),
            "X_test_processed": AssetIn(key="X_test_processed"),
            "y_test": AssetIn(key="y_test")
        },
        io_manager_key="file_io_manager",
    )
    def test_model_asset(context, **kwargs):
        """Evaluate a trained model on processed test data."""
        # Get the trained model data
        trained_model_data = kwargs[f"{model_name}_trained"]
        X_test_processed = kwargs["X_test_processed"]
        y_test = kwargs["y_test"]
        
        logger = context.log
        
        # Extract the trained model
        model = trained_model_data["model"]
        train_metrics = trained_model_data["train_metrics"]
        
        logger.info(f"Evaluating {model_name} model on test data...")
        logger.info(f"Test data shape: {X_test_processed.shape}")
        
        # Make predictions on test data
        test_preds = model.predict(X_test_processed)
        
        # Calculate test metrics - Convert numpy types to Python native types
        test_metrics = {
            "test_rmse": float(np.sqrt(mean_squared_error(y_test, test_preds))),
            "test_r2": float(r2_score(y_test, test_preds)),
            "test_mae": float(mean_absolute_error(y_test, test_preds)),
        }
        
        # Combine all metrics
        all_metrics = {**train_metrics, **test_metrics}
        
        logger.info(f"Test metrics for {model_name}: {test_metrics}")
        logger.info(f"All metrics for {model_name}: {all_metrics}")
        
        # Calculate overfitting ratio safely
        overfitting_ratio = float(test_metrics["test_rmse"] / train_metrics["train_rmse"]) if train_metrics["train_rmse"] > 0 else 1.0
        
        # Add metadata
        context.add_output_metadata({
            "model_type": MetadataValue.text(model_name),
            "test_rmse": MetadataValue.float(test_metrics["test_rmse"]),
            "test_r2": MetadataValue.float(test_metrics["test_r2"]),
            "test_mae": MetadataValue.float(test_metrics["test_mae"]),
            "train_rmse": MetadataValue.float(train_metrics["train_rmse"]),
            "train_r2": MetadataValue.float(train_metrics["train_r2"]),
            "overfitting_ratio": MetadataValue.float(overfitting_ratio),
            "test_samples": MetadataValue.int(len(X_test_processed))
        })
        
        # Convert predictions to Python list with native float types
        predictions_list = [float(pred) for pred in test_preds] if hasattr(test_preds, '__iter__') else [float(test_preds)]
        
        # Return complete model evaluation results
        return {
            "model": model,
            "all_metrics": all_metrics,
            "train_metrics": train_metrics,
            "test_metrics": test_metrics,
            "model_params": trained_model_data["model_params"],
            "model_type": model_name,
            "predictions": predictions_list  # For potential future analysis
        }
    
    return train_model_asset, test_model_asset

# Create training and testing assets for different models
random_forest_train, random_forest_test = create_model_asset(
    "random_forest", 
    RandomForestRegressor, 
    {"n_estimators": 100, "max_depth": 10, "random_state": 42}
)

xgboost_train, xgboost_test = create_model_asset(
    "xgboost", 
    XGBRegressor, 
    {"n_estimators": 100, "learning_rate": 0.1, "random_state": 42}
)

gradient_boosting_train, gradient_boosting_test = create_model_asset(
    "gradient_boosting",
    GradientBoostingRegressor,
    {"n_estimators": 100, "learning_rate": 0.1, "max_depth": 6, "random_state": 42}
)

ridge_train, ridge_test = create_model_asset(
    "ridge",
    Ridge,
    {"alpha": 1.0, "random_state": 42}
)
