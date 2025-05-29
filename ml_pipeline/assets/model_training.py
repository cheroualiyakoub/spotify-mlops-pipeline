from dagster import asset, AssetIn, Output, MetadataValue
import mlflow.artifacts
import mlflow.sklearn
import mlflow.xgboost
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge, Lasso
from xgboost import XGBClassifier
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error, accuracy_score, precision_score, recall_score, f1_score
import pickle
import mlflow
import mlflow.sklearn
import os
import time
from typing import Dict, Any, Tuple
import logging
from dataclasses import dataclass, field

@dataclass
class ModelConfig:
    """Configuration for model training"""
    name: str
    model_class: Any
    params: Dict[str, Any]
    timeout_seconds: int = 600  # Default timeout of 10 minutes
    classification: bool = False  # Whether this is a classification task
    validation_threshold: Dict[str, float] = field(default_factory=lambda: {
        "min_accuracy": 0.7,
        "min_f1": 0.65,
        "overfitting_threshold": 1.5  # Max allowed ratio of test/train error
    })

# Define model configurations
XGBOOST_CONFIG = ModelConfig(
    name="xgboost",
    model_class=XGBClassifier,
    params={"n_estimators": 100, "learning_rate": 0.1, "random_state": 42},
    classification=True
)

# Factory function to create model training assets
def create_model_asset(model_config: ModelConfig):
    """
    Factory function to create separate training and testing assets for a model
    
    Args:
        model_config: Configuration for the model to train
    
    Returns:
        tuple: (training_asset, testing_asset)
    """
    
    # Training Asset
    @asset(
        name=f"{model_config.name}_trained",
        group_name="models",
        ins={
            "X_train_processed": AssetIn(key="X_train_processed"),
            "y_train": AssetIn(key="y_train"),
            "preprocessor": AssetIn(key="preprocessor")
        },
        io_manager_key="file_io_manager",
        required_resource_keys={"mlflow_tracking"}
    )
    def train_model_asset(context, X_train_processed, y_train, preprocessor):
        """Train a model on processed training data with MLflow tracking."""
        logger = context.log
        mlflow_resource = context.resources.mlflow_tracking
        
        logger.info(f"Training {model_config.name} model...")
        logger.info(f"Training data shape: {X_train_processed.shape}")
        logger.info(f"Target shape: {y_train.shape}")
        
        # Setup MLflow experiment
        experiment_name = mlflow_resource.setup_experiment(context, experiment_suffix=model_config.name)
        
        # Start MLflow run with tags for easier filtering
        with mlflow.start_run(run_name=f"{model_config.name}_training") as run:
            run_id = run.info.run_id
            
            # Add tags for better organization
            mlflow.set_tag("model_type", model_config.name)
            mlflow.set_tag("pipeline_stage", "training")
            mlflow.set_tag("dagster_run_id", context.run_id)
            
            # Log model parameters
            mlflow.log_params(model_config.params)
            mlflow.log_param("model_type", model_config.name)
            mlflow.log_param("training_samples", len(X_train_processed))
            mlflow.log_param("features_count", X_train_processed.shape[1])
            
            try:
                # Set a timeout for model training
                start_time = time.time()
                
                # Initialize and train the model
                model = model_config.model_class(**model_config.params)
                model.fit(X_train_processed, y_train)
                
                # Track training duration
                training_duration = time.time() - start_time
                mlflow.log_metric("training_duration_seconds", training_duration)
                
                if training_duration > model_config.timeout_seconds:
                    logger.warning(f"Model training exceeded timeout ({training_duration:.1f}s > {model_config.timeout_seconds}s)")
                
                # Make predictions on training data for training metrics
                train_preds = model.predict(X_train_processed)
                
                # Calculate metrics based on model type
                train_metrics = {}
                
                # Classification metrics if applicable
                if model_config.classification:
                    train_metrics.update({
                        "train_accuracy": float(accuracy_score(y_train, train_preds)),
                        "train_precision": float(precision_score(y_train, train_preds, zero_division=0)),
                        "train_recall": float(recall_score(y_train, train_preds, zero_division=0)),
                        "train_f1": float(f1_score(y_train, train_preds, zero_division=0))
                    })
                
                # Regression metrics (may still be useful for classification)
                train_metrics.update({
                    "train_rmse": float(np.sqrt(mean_squared_error(y_train, train_preds))),
                    "train_r2": float(r2_score(y_train, train_preds)),
                    "train_mae": float(mean_absolute_error(y_train, train_preds)),
                })
                
                # Log training metrics to MLflow
                mlflow.log_metrics(train_metrics)
                
                # Create a full pipeline with preprocessor and model
                full_pipeline = {
                    "preprocessor": preprocessor,
                    "model": model,
                }
                
                # Log the full pipeline
                mlflow_resource.log_model(
                    context,
                    full_pipeline,
                    f"{model_config.name}_full_pipeline",
                    registered_model_name=f"spotify_{model_config.name}_pipeline"
                )

                # Log just the model component for comparison/analysis
                mlflow_resource.log_model(
                    context,
                    model,
                    f"{model_config.name}_model_only",
                    registered_model_name=f"spotify_{model_config.name}_model"
                )

                # Log feature importance if available
                try:
                    if hasattr(model, 'feature_importances_'):
                        feature_imp = pd.DataFrame({
                            'Feature': X_train_processed.columns,
                            'Importance': model.feature_importances_
                        }).sort_values('Importance', ascending=False)
                        
                        # Log feature importance as a table artifact
                        feature_imp_path = f"/tmp/feature_imp_{run_id}.csv"
                        feature_imp.to_csv(feature_imp_path, index=False)
                        mlflow.log_artifact(feature_imp_path, "feature_importance")
                        os.remove(feature_imp_path)
                        
                        # Log top features as parameters for easy viewing
                        top_features = feature_imp.head(5)['Feature'].tolist()
                        mlflow.log_param("top_features", ", ".join(top_features))
                except Exception as e:
                    logger.warning(f"Could not log feature importance: {str(e)}")
                
                logger.info(f"MLflow run ID: {run_id}")
                logger.info(f"Training metrics for {model_config.name}: {train_metrics}")
                
            except Exception as e:
                logger.error(f"Error training {model_config.name} model: {str(e)}")
                mlflow.log_param("training_error", str(e))
                mlflow.set_tag("training_failed", "true")
                raise
        
        # Remove explicit mlflow.end_run() to keep the run active
        
        # Add metadata to Dagster
        context.add_output_metadata({
            "model_type": MetadataValue.text(model_config.name),
            "model_params": MetadataValue.json(model_config.params),
            "train_metrics": MetadataValue.json(train_metrics),
            "training_duration_seconds": MetadataValue.float(training_duration),
            "training_samples": MetadataValue.int(len(X_train_processed)),
            "features_count": MetadataValue.int(X_train_processed.shape[1]),
            "mlflow_run_id": MetadataValue.text(run_id),
            "mlflow_experiment": MetadataValue.text(experiment_name[0] if isinstance(experiment_name, tuple) else experiment_name)
        })
        
        # Return trained model with training metrics and MLflow info
        return {
            "model": model,
            "train_metrics": train_metrics,
            "model_params": model_config.params,
            "model_type": model_config.name,
            "model_config": model_config,
            "mlflow_run_id": run_id,
            "mlflow_experiment": experiment_name[0] if isinstance(experiment_name, tuple) else experiment_name
        }
    
    # Testing Asset
    @asset(
        deps=[f"{model_config.name}_trained", "X_test_processed", "y_test"],
        name=f"{model_config.name}_evaluated",
        group_name="models",
        ins={
            f"{model_config.name}_trained": AssetIn(key=f"{model_config.name}_trained"),
            "X_test_processed": AssetIn(key="X_test_processed"),
            "y_test": AssetIn(key="y_test")
        },
        io_manager_key="file_io_manager",
        required_resource_keys={"mlflow_tracking"}
    )
    def test_model_asset(context, **kwargs):
        """Evaluate a trained model on processed test data with MLflow tracking."""
        # Store model_config from outer scope for use in key
        outer_model_config = model_config  # Capture the outer scope model_config
        model_config_name = outer_model_config.name
        
        # Get the trained model data
        trained_model_data = kwargs[f"{model_config_name}_trained"]
        X_test_processed = kwargs["X_test_processed"]
        y_test = kwargs["y_test"]
        
        logger = context.log
        mlflow_resource = context.resources.mlflow_tracking
        
        # Extract the trained model and MLflow info
        model = trained_model_data["model"]

        # Get model_config from trained_model_data, with fallback to outer scope
        current_model_config = trained_model_data.get("model_config", outer_model_config)
        train_metrics = trained_model_data["train_metrics"]
        parent_run_id = trained_model_data["mlflow_run_id"]
        experiment_name = trained_model_data["mlflow_experiment"]
        
        logger.info(f"Evaluating {current_model_config.name} model on test data...")
        logger.info(f"Test data shape: {X_test_processed.shape}")
        
        # Set the experiment
        logger.info(f"Starting MLflow evaluation run: before {current_model_config.name}_evaluation")
        logger.info(f"parent_run_id: {parent_run_id}")
        
        # Start a new MLflow run for evaluation (child run of training)
        with mlflow.start_run(run_id=parent_run_id) as run:
            eval_run_id = run.info.run_id

            logger.info(f"Starting MLflow evaluation run: {run.info.run_name} (ID: {eval_run_id})")

            # Add tags
            mlflow.set_tag("model_type", current_model_config.name)
            mlflow.set_tag("pipeline_stage", "evaluation")
            mlflow.set_tag("parent_run_id", parent_run_id)
            mlflow.set_tag("dagster_run_id", context.run_id)
            
            # Log reference to parent training run
            mlflow.log_param("parent_run_id", parent_run_id)
            mlflow.log_param("model_type", current_model_config.name)
            mlflow.log_param("test_samples", len(X_test_processed))
            
            try:
                # Make predictions on test data
                test_preds = model.predict(X_test_processed)
                
                # Calculate metrics based on model type
                test_metrics = {}
                validation_results = {}
                
                # Classification metrics if applicable
                if current_model_config.classification:
                    test_metrics.update({
                        "test_accuracy": float(accuracy_score(y_test, test_preds)),
                        "test_precision": float(precision_score(y_test, test_preds, zero_division=0)),
                        "test_recall": float(recall_score(y_test, test_preds, zero_division=0)),
                        "test_f1": float(f1_score(y_test, test_preds, zero_division=0))
                    })
                    
                    # Validate against thresholds
                    validation_results["accuracy_valid"] = test_metrics["test_accuracy"] >= current_model_config.validation_threshold["min_accuracy"]
                    validation_results["f1_valid"] = test_metrics["test_f1"] >= current_model_config.validation_threshold["min_f1"]
                
                # Always calculate regression metrics
                test_metrics.update({
                    "test_rmse": float(np.sqrt(mean_squared_error(y_test, test_preds))),
                    "test_r2": float(r2_score(y_test, test_preds)),
                    "test_mae": float(mean_absolute_error(y_test, test_preds)),
                })
                
                # Calculate overfitting ratio safely
                rmse_ratio = float(test_metrics["test_rmse"] / train_metrics["train_rmse"]) if train_metrics["train_rmse"] > 0 else 1.0
                
                # Combine all metrics for MLflow logging
                all_metrics = {
                    **train_metrics,
                    **test_metrics,
                    "overfitting_ratio": rmse_ratio
                }
                
                # Validate overfitting
                validation_results["overfitting_valid"] = rmse_ratio <= current_model_config.validation_threshold["overfitting_threshold"]
                
                # Log all metrics and validation results to MLflow
                mlflow.log_metrics(all_metrics)
                for key, value in validation_results.items():
                    mlflow.set_tag(key, str(value))
                
                # Overall validation status
                all_valid = all(validation_results.values())
                mlflow.set_tag("model_passed_validation", str(all_valid))
                
                # Log confusion matrix if classification model
                if current_model_config.classification:
                    try:
                        from sklearn.metrics import confusion_matrix
                        import matplotlib.pyplot as plt
                        import seaborn as sns
                        
                        cm = confusion_matrix(y_test, test_preds)
                        plt.figure(figsize=(8, 6))
                        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
                        plt.xlabel('Predicted')
                        plt.ylabel('Actual')
                        plt.title('Confusion Matrix')
                        
                        # Save and log the confusion matrix
                        cm_path = f"/tmp/confusion_matrix_{eval_run_id}.png"
                        plt.savefig(cm_path)
                        mlflow.log_artifact(cm_path, "evaluation")
                        os.remove(cm_path)
                        plt.close()  # Close the plot to free memory
                    except Exception as e:
                        logger.warning(f"Could not create confusion matrix: {str(e)}")
                
                # Log predictions as artifacts
                predictions_sample = test_preds[:100] if len(test_preds) > 100 else test_preds
                
                # Convert predictions to list of native Python types for JSON serialization
                predictions_list = [float(pred) for pred in predictions_sample]
                
                # Create a DataFrame of predictions vs actual
                pred_df = pd.DataFrame({
                    'actual': y_test.iloc[:100].values if len(y_test) > 100 else y_test.values,
                    'predicted': predictions_sample
                })
                
                # Save and log the predictions comparison
                pred_path = f"/tmp/predictions_{eval_run_id}.csv"
                pred_df.to_csv(pred_path, index=False)
                mlflow.log_artifact(pred_path, "evaluation")
                os.remove(pred_path)
                
                logger.info(f"MLflow evaluation run ID: {eval_run_id}")
                logger.info(f"Test metrics for {current_model_config.name}: {test_metrics}")
                logger.info(f"Validation results: {validation_results}")
                logger.info(f"Model passed all validation: {all_valid}")
                
            except Exception as e:
                logger.error(f"Error evaluating {current_model_config.name} model: {str(e)}")
                mlflow.log_param("evaluation_error", str(e))
                mlflow.set_tag("evaluation_failed", "true")
                raise
    
        # Add metadata to Dagster
        context.add_output_metadata({
            "model_type": MetadataValue.text(current_model_config.name),
            "test_metrics": MetadataValue.json(test_metrics),
            "train_metrics": MetadataValue.json(train_metrics),
            "overfitting_ratio": MetadataValue.float(rmse_ratio),
            "passed_validation": MetadataValue.bool(all_valid),
            "test_samples": MetadataValue.int(len(X_test_processed)),
            "mlflow_eval_run_id": MetadataValue.text(eval_run_id),
            "mlflow_experiment": MetadataValue.text(experiment_name)
        })
        
        # Return complete model evaluation results
        return {
            "model": model,
            "all_metrics": all_metrics,
            "train_metrics": train_metrics,
            "test_metrics": test_metrics,
            "model_params": trained_model_data["model_params"],
            "model_type": current_model_config.name,
            "predictions": [float(pred) for pred in test_preds],  # Convert to Python native types
            "overfitting_ratio": rmse_ratio,
            "validation_results": validation_results,
            "model_passed_validation": all_valid,
            "mlflow_eval_run_id": eval_run_id,
            "mlflow_experiment": experiment_name
        }
    
    return train_model_asset, test_model_asset


# Create XGBoost model assets using the configuration
xgboost_train, xgboost_test = create_model_asset(XGBOOST_CONFIG)





@asset(name="enhanced_debug_mlflow", group_name="debug")
def enhanced_debug_mlflow(context):
    """Enhanced debugging of MLflow artifact storage with MinIO."""
    import mlflow
    import requests
    import tempfile
    import os
    import boto3
    import socket
    
    logger = context.log
    
    # Detect if we're running in Docker or not
    in_container = os.path.exists('/.dockerenv')
    logger.info(f"Running in Docker container: {in_container}")
    
    # Set appropriate URLs based on environment
    if in_container:
        mlflow_url = "http://mlflow:5000"
        minio_url = "http://minio:9000"
    else:
        mlflow_url = "http://localhost:5000"
        minio_url = "http://localhost:9000"
    
    logger.info(f"Using MLflow URL: {mlflow_url}")
    logger.info(f"Using MinIO URL: {minio_url}")
    
    mlflow.set_tracking_uri(mlflow_url)
    
    # Test 1: Check MLflow server health
    try:
        response = requests.get(f"{mlflow_url}/health")
        logger.info(f"✓ MLflow health check: {response.status_code}")
    except Exception as e:
        logger.error(f"✗ MLflow health check failed: {e}")
        return {"status": "failed", "error": "MLflow not accessible"}
    
    # Test 2: Check MinIO connectivity
    try:
        response = requests.get(f"{minio_url}/minio/health/live")
        logger.info(f"✓ MinIO health check: {response.status_code}")
    except Exception as e:
        logger.error(f"✗ MinIO health check failed: {e}")
        return {"status": "failed", "error": "MinIO not accessible"}
    
    # Test 3: Check if MinIO bucket exists using boto3
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_url,
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        # List buckets
        buckets = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in buckets['Buckets']]
        logger.info(f"✓ Available buckets: {bucket_names}")
        
        if 'mlflow-artifacts' in bucket_names:
            logger.info("✓ mlflow-artifacts bucket exists")
            
            # Try to list objects in the bucket
            try:
                objects = s3_client.list_objects_v2(Bucket='mlflow-artifacts')
                object_count = objects.get('KeyCount', 0)
                logger.info(f"✓ mlflow-artifacts bucket has {object_count} objects")
            except Exception as e:
                logger.info(f"✓ mlflow-artifacts bucket is empty or not accessible: {e}")
        else:
            logger.error("✗ mlflow-artifacts bucket does not exist!")
            return {"status": "failed", "error": "mlflow-artifacts bucket missing"}
            
    except Exception as e:
        logger.error(f"✗ S3/MinIO connection failed: {e}")
        return {"status": "failed", "error": f"S3 connection error: {e}"}
    
    # Set AWS environment variables for MLflow
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = minio_url
    
    # Test 4: Create MLflow run and check artifact URI
    try:
        with mlflow.start_run(run_name="enhanced_debug_test") as run:
            run_id = run.info.run_id
            
            # Check the artifact URI BEFORE logging anything
            run_details = mlflow.get_run(run_id)
            artifact_uri = run_details.info.artifact_uri
            logger.info(f"🔍 Artifact URI: {artifact_uri}")
            
            # Check if it's using S3 or local filesystem
            if artifact_uri.startswith('s3://'):
                logger.info("✓ MLflow is configured to use S3 storage")
                storage_type = "S3"
            elif artifact_uri.startswith('/'):
                logger.error("✗ MLflow is using local filesystem instead of S3!")
                storage_type = "Local Filesystem"
            else:
                logger.warning(f"? Unknown storage type: {artifact_uri}")
                storage_type = "Unknown"
            
            # Test 5: Try to log an artifact
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                f.write(f"Enhanced debug test artifact\nRun ID: {run_id}\nStorage: {storage_type}")
                temp_path = f.name
            
            try:
                logger.info("📤 Attempting to log artifact...")
                mlflow.log_artifact(temp_path, "debug_enhanced")
                logger.info("✓ Artifact logged successfully")
                
                # Log some parameters and metrics
                mlflow.log_param("storage_type", storage_type)
                mlflow.log_param("bucket_exists", 'mlflow-artifacts' in bucket_names)
                mlflow.log_param("environment", "container" if in_container else "local")
                mlflow.log_metric("test_success", 1.0)
                
                # Verify artifact was actually stored
                if storage_type == "S3":
                    try:
                        # Check if artifact exists in MinIO
                        # Experiment ID is typically "0" for default experiment,
                        # but we'll check both "0" and "1" to be safe
                        for exp_id in ["0", "1"]:
                            prefix = f"{exp_id}/{run_id}/artifacts/debug_enhanced/"
                            objects = s3_client.list_objects_v2(
                                Bucket='mlflow-artifacts', 
                                Prefix=prefix
                            )
                            if objects.get('KeyCount', 0) > 0:
                                logger.info(f"✓ Artifact verified in MinIO storage under prefix {prefix}")
                                verification_status = "verified_in_minio"
                                break
                        else:  # This else belongs to the for loop
                            logger.warning("? Artifact not found in MinIO (may take a moment)")
                            verification_status = "not_found_in_minio"
                    except Exception as e:
                        logger.warning(f"? Could not verify artifact in MinIO: {e}")
                        verification_status = "verification_failed"
                else:
                    verification_status = "local_storage"
                
                return {
                    "status": "success",
                    "mlflow_run_id": run_id,
                    "artifact_uri": artifact_uri,
                    "storage_type": storage_type,
                    "bucket_exists": 'mlflow-artifacts' in bucket_names,
                    "verification_status": verification_status,
                    "available_buckets": bucket_names,
                    "environment": "container" if in_container else "local"
                }
                
            except Exception as e:
                logger.error(f"✗ Artifact logging failed: {e}")
                mlflow.log_param("error", str(e))
                mlflow.log_metric("test_success", 0.0)
                return {
                    "status": "artifact_failed",
                    "error": str(e),
                    "storage_type": storage_type,
                    "environment": "container" if in_container else "local"
                }
            finally:
                os.unlink(temp_path)
                
    except Exception as e:
        logger.error(f"✗ MLflow run creation failed: {e}")
        return {"status": "mlflow_failed", "error": str(e)}



@asset(name="mlflow_test_asset", group_name="debug", required_resource_keys={"mlflow_tracking"})
def mlflow_test_asset(context):
    """Test MLflow artifact storage with MinIO and verify artifact serving."""
    import mlflow
    import tempfile
    import os
    import time
    import requests
    import json
    import boto3
    from urllib.parse import urlparse
    
    logger = context.log
    mlflow_resource = context.resources.mlflow_tracking
    
    logger.info("Running MLflow testing asset...")
    
    # Setup MLflow experiment
    experiment_name = "mlflow_artifact_test"
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(experiment_name)
        else:
            experiment_id = experiment.experiment_id
    except Exception as e:
        logger.error(f"Error setting up MLflow experiment: {e}")
        experiment_id = "0"  # Use default experiment if there's an error
        
    # Start MLflow run with descriptive name
    with mlflow.start_run(experiment_id=experiment_id, run_name="artifact_storage_test") as run:
        run_id = run.info.run_id
        logger.info(f"Started MLflow run with ID: {run_id}")
        
        # Create test artifact with unique content
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_path = os.path.join(temp_dir, "test_artifact.txt")
            with open(artifact_path, "w") as f:
                timestamp = time.time()
                f.write(f"MLflow test artifact\nRun ID: {run_id}\nTimestamp: {timestamp}")
            
            # Log the artifact
            mlflow.log_artifact(artifact_path, "test_artifacts")
            logger.info(f"Successfully logged test artifact: {artifact_path}")
            
            # Get artifact location
            artifact_uri = mlflow.get_artifact_uri()
            logger.info(f"Artifact URI: {artifact_uri}")
            
            # Check if the artifact URI is using S3 (MinIO)
            is_s3 = artifact_uri.startswith("s3://")
            logger.info(f"Using S3/MinIO storage: {is_s3}")
            
            # Check if artifact has been stored in MinIO via boto3
            if is_s3:
                try:
                    # Extract the bucket and key from the S3 URI
                    parsed_uri = urlparse(artifact_uri)
                    bucket_name = parsed_uri.netloc
                    key_prefix = parsed_uri.path.lstrip('/') + "/test_artifacts/"
                    
                    # Connect to MinIO
                    s3_client = boto3.client(
                        's3',
                        endpoint_url="http://minio:9000" if os.path.exists('/.dockerenv') else "http://localhost:9000",
                        aws_access_key_id="minioadmin",
                        aws_secret_access_key="minioadmin"
                    )
                    
                    # List objects in MinIO with the prefix
                    response = s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=key_prefix
                    )
                    
                    # Check if objects exist
                    if 'Contents' in response:
                        logger.info(f"Found {len(response['Contents'])} artifact(s) in MinIO bucket '{bucket_name}' with prefix '{key_prefix}'")
                        for item in response['Contents']:
                            logger.info(f"Found artifact: {item['Key']}")
                    else:
                        logger.warning(f"No artifacts found in MinIO bucket '{bucket_name}' with prefix '{key_prefix}'")
                except Exception as e:
                    logger.error(f"Error checking MinIO: {e}")
            
            # Get a direct URL to the artifact through MLflow's API
            mlflow_url = "http://mlflow:5000" if os.path.exists('/.dockerenv') else "http://localhost:5000"
            artifact_path_mlflow = f"test_artifacts/test_artifact.txt"
            artifacts_api_url = f"{mlflow_url}/api/2.0/mlflow-artifacts/artifacts/{run_id}/artifacts/{artifact_path_mlflow}"
            logger.info(f"MLflow artifact URL: {artifacts_api_url}")
            
            # Try to fetch the artifact through MLflow to verify serving
            try:
                # We'll need to wait a bit for MinIO to sync and MLflow to make it available
                time.sleep(5)  # Wait 5 seconds
                
                response = requests.get(artifacts_api_url)
                if response.status_code == 200:
                    logger.info("✅ Successfully retrieved artifact through MLflow API!")
                    logger.info(f"Artifact content preview: {response.text[:100]}...")
                else:
                    logger.warning(f"⚠️ Could not retrieve artifact through MLflow API. Status code: {response.status_code}")
            except Exception as e:
                logger.error(f"Error accessing artifact via MLflow API: {e}")
                
            # Record metrics for verification
            mlflow.log_metric("verification_timestamp", timestamp)
            mlflow.log_metric("is_s3_storage", 1 if is_s3 else 0)
    
    # Return information about the test
    result = {
        "mlflow_run_id": run_id,
        "mlflow_url": f"http://localhost:5000/#/experiments/{experiment_id}/runs/{run_id}",
        "artifact_uri": artifact_uri,
        "test_status": "SUCCESS" if is_s3 else "FAILURE",
        "storage_type": "MinIO" if is_s3 else "Local",
        "artifact_test_path": artifact_path_mlflow
    }
    
    logger.info(f"MLflow test successful! View results at: {result['mlflow_url']}")
    return result