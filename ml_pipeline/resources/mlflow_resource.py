from dagster import ConfigurableResource
from pathlib import Path
import os
import tempfile
import shutil
from dagster import ConfigurableResource

class MLflowTrackingResource(ConfigurableResource):
    """Resource for MLflow tracking with S3/MinIO artifact storage."""
    tracking_uri: str = "http://mlflow:5000"
    base_experiment_name: str = "spotify_popularity_prediction"
    enable_autolog: bool = True

    def setup_experiment(self, context, experiment_suffix=None):
        import mlflow

        try:
            mlflow.set_tracking_uri(self.tracking_uri)
            context.log.info(f"Set MLflow tracking URI to: {self.tracking_uri}")

            if self.enable_autolog:
                mlflow.autolog(log_datasets=False)
                context.log.info("MLflow autologging enabled")

            experiment_name = self.base_experiment_name
            if experiment_suffix:
                experiment_name = f"{experiment_name}_{experiment_suffix}"

            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(experiment_name)
                context.log.info(f"Created experiment '{experiment_name}'")
            else:
                experiment_id = experiment.experiment_id
                context.log.info(f"Using existing experiment '{experiment_name}' (ID: {experiment_id})")

            mlflow.set_experiment(experiment_name)
            return experiment_name

        except Exception as e:
            context.log.error(f"Error setting up MLflow experiment: {e}")
            raise

    def log_model(self, context, model, artifact_path, registered_model_name=None):
        import mlflow
        import mlflow.sklearn
        import mlflow.xgboost

        try:
            context.log.info(f"Logging model to MLflow at path '{artifact_path}'")
            if hasattr(model, 'booster') and 'xgboost' in str(type(model)).lower():
                mlflow.xgboost.log_model(
                    model,
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name if registered_model_name else None
                )
                context.log.info(f"Logged XGBoost model to MLflow at '{artifact_path}'")
            else:
                mlflow.sklearn.log_model(
                    model,
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name if registered_model_name else None
                )
                context.log.info(f"Logged sklearn model to MLflow at '{artifact_path}'")

            run_id = mlflow.active_run().info.run_id
            context.log.info(f"Active run ID: {run_id}")
            context.log.info(f"Artifact URI: {mlflow.get_artifact_uri(artifact_path)}")
            return True
        except Exception as e:
            context.log.error(f"Error logging model to MLflow: {e}")
            context.log.error(f"Model type: {type(model)}")
            raise

    def load_model(self, context, run_id, model_path):
        import mlflow
        import mlflow.pyfunc

        try:
            context.log.info(f"Loading model from MLflow run {run_id}, path '{model_path}'")
            model_uri = f"runs:/{run_id}/{model_path}"
            model = mlflow.pyfunc.load_model(model_uri)
            context.log.info(f"Successfully loaded model from MLflow")
            return model
        except Exception as e:
            context.log.error(f"Error loading model from MLflow: {e}")
            context.log.error(f"Model URI that failed: runs:/{run_id}/{model_path}")
            try:
                import mlflow.tracking
                client = mlflow.tracking.MlflowClient()
                artifacts = client.list_artifacts(run_id, model_path)
                context.log.error(f"Available artifacts: {artifacts}")
            except Exception as nested_e:
                context.log.error(f"Could not list artifacts: {nested_e}")
            raise
            