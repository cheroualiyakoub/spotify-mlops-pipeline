from dagster import ConfigurableResource, StringSource

class MLflowTrackingResource(ConfigurableResource):
    """Resource for MLflow tracking configuration"""
    tracking_uri: str = StringSource(
        default="http://localhost:5000",
        description="MLflow tracking server URI"
    )
    base_experiment_name: str = StringSource(
        default="spotify_popularity_prediction",
        description="Base MLflow experiment name (models can specify suffixes)"
    )
    
    def setup_experiment(self, context, experiment_suffix=None):
        """Set up MLflow experiment with optional suffix"""
        import mlflow
        mlflow.set_tracking_uri(self.tracking_uri)
        
        # Generate experiment name (with optional suffix)
        experiment_name = self.base_experiment_name
        if experiment_suffix:
            experiment_name = f"{experiment_name}_{experiment_suffix}"
        
        # Create or get experiment
        try:
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(experiment_name)
                context.log.info(f"Created new experiment '{experiment_name}' with ID {experiment_id}")
            else:
                context.log.info(f"Using existing experiment '{experiment_name}' with ID {experiment.experiment_id}")
                
            return experiment_name
            
        except Exception as e:
            context.log.error(f"Error setting up MLflow experiment: {e}")
            raise
