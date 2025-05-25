# Fixed IO Manager Code
from dagster import IOManager, io_manager, get_dagster_logger, Output
from lakefs_spec import LakeFSFileSystem
import pandas as pd
from dagster import IOManager, io_manager, ConfigurableResource
from typing import Optional, Dict, Any
from dataclasses import dataclass
from lakefs_client.exceptions import NotFoundException
from lakefs_client.models import BranchCreation
import io

@dataclass
class LakeFSConfig:
    """Configuration for LakeFS operations"""
    repo: str
    branch: str
    path :str
    commit_message: Optional[str] = None
    auto_commit: bool = False
    metadata: Optional[Dict[str, Any]] = None
    auto_create_branches: bool = True
    source_branch: str = "main"

class DynamicLakeFSIOManager(IOManager):
    def __init__(self, fs, client, default_repo: str = "spotify-repo", default_branch: str = "main", auto_create_branches: bool = True, source_branch: str = "main"):
        self.fs = fs
        self.client = client
        self.default_repo = default_repo
        self.default_branch = default_branch
        self.auto_create_branches = auto_create_branches
        self.source_branch = source_branch

    def _ensure_branch_exists(self, context, repository: str, branch: str):
        """Create branch if missing with safety checks"""
        if not self.auto_create_branches:
            context.log.debug("Auto branch creation disabled")
            return

        try:
            # Check for existing branch
            self.client.branches.get_branch(
                repository=repository,
                branch=branch
            )
            context.log.debug(f"Branch {branch} exists")
        except NotFoundException:
            context.log.info(f"Creating new branch {repository}/{branch} from {self.source_branch}")
            
            try:
                # Verify source branch exists first
                self.client.branches.get_branch(
                    repository=repository,
                    branch=self.source_branch
                )
            except NotFoundException:
                raise ValueError(
                    f"Source branch {self.source_branch} not found in {repository}. "
                    "Cannot create new branch."
                ) from None

            # Create new branch
            try:
                self.client.branches.create_branch(
                    repository=repository,
                    branch_creation=BranchCreation(
                        name=branch,
                        source=self.source_branch
                    )
                )
                context.log.info(f"Successfully created branch {branch}")
            except Exception as e:
                context.log.error(f"Failed to create branch {branch}: {str(e)}")
                raise

    def _get_config_from_context(self, context, runtime_config = {}) -> LakeFSConfig:
        get_dagster_logger().info(f"Fetching LakeFS config for asset {context.asset_key}")

        # Get static config from definition_metadata
        static_config = {}
        if hasattr(context, 'definition_metadata') and context.definition_metadata:
            static_config = context.definition_metadata.get('lakefs_config', {})

        get_dagster_logger().info(f"Static LakeFS config: {static_config}")
        get_dagster_logger().info(f"Runtime LakeFS config: {runtime_config}")

        final_config = {**static_config, **runtime_config}

        if hasattr(context, 'partition_key') and context.partition_key:
            partition_key = context.partition_key

            if 'path' in final_config:
                final_config['path'] = final_config['path'].format(
                    partition_key=partition_key,
                    year=partition_key
                )
            if 'commit_message' in final_config:
                final_config['commit_message'] = final_config['commit_message'].format(
                    partition_key=partition_key,
                    year=partition_key
                )

        return LakeFSConfig(
            repo=final_config.get('repo', self.default_repo),
            branch=final_config.get('branch', self.default_branch),
            path=final_config.get('path'),
            commit_message=final_config.get('commit_message'),
            auto_commit=final_config.get('auto_commit', False),
            metadata=final_config.get('metadata', {})
        )


    def handle_output(self, context, obj):
        """Simple CSV upload using custom path from lakefs_config"""
        logger = get_dagster_logger()
        
        logger.info(f"Handling output for {context.asset_key}")


        if isinstance(obj, pd.DataFrame):
            logger.info(f"📊 DataFrame shape: {obj.shape}")
            config = self._get_config_from_context(context)
            logger.info(f"Using LakeFS config: {config}")
            
            self._ensure_branch_exists(
                context=context,   
                repository=config.repo,
                branch=config.branch
            )

            # Use custom path if provided, otherwise use asset name
            if hasattr(config, 'path') and config.path:
                file_path = config.path
            else:
                file_path = f"{'/'.join(context.asset_key.path)}.csv"
            
            context.log.info(f"📤 Uploading CSV: {list(obj.shape)} to {config.repo}/{config.branch}/{file_path}")
            
            # Rest of your upload code stays the same...
            csv_content = obj.to_csv(index=False)
            csv_bytes = csv_content.encode('utf-8')
            csv_file = io.BytesIO(csv_bytes)
            
            self.client.objects.upload_object(
                repository=config.repo,
                branch=config.branch,
                path=file_path,  # Use the custom path
                content=csv_file
            )
            
            context.log.info(f"✅ Upload successful!")
            
            context.add_output_metadata({
                "repo": config.repo,
                "branch": config.branch,
                "path": file_path,  # Store the actual path used
                "file_format": "csv",
                "size_bytes": len(csv_bytes),
                "shape": list(obj.shape)
            })

    def load_input(self, context):
        """Simple CSV download using upstream metadata only"""
        
        logger = get_dagster_logger()

        logger.info(f"Loading input for {context.asset_key} from upstream output")
        
        upstream_metadata = context.upstream_output.metadata
        logger.info(f"Upstream metadata: {upstream_metadata}")

        # Get file info from upstream metadata (stored by handle_output)
        lakef_config = upstream_metadata.get("lakefs_config")
        repo = lakef_config.get("repo", self.default_repo)
        branch = lakef_config.get("branch", self.default_branch)
        file_path = lakef_config.get("path")
        
        if not file_path:
            # Fallback: construct from asset name
            upstream_asset_key = context.upstream_output.asset_key
            file_path = f"{'/'.join(upstream_asset_key.path)}.csv"
        
        context.log.info(f"📥 Loading from upstream metadata: {repo}/{branch}/{file_path}")
        
        try:
            response = self.client.objects.get_object(
                repository=repo,
                ref=branch,
                path=file_path
            )
            
            csv_content = response.read().decode('utf-8')
            context.log.info(f"✅ Downloaded {len(csv_content)} characters")
            
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content))
            context.log.info(f"✅ Loaded DataFrame: {df.shape}")
            
            return df
            
        except Exception as e:
            context.log.error(f"❌ Download failed from {repo}/{branch}/{file_path}: {e}")
            
            # Debug: show what metadata we have
            context.log.info(f"🔍 Available upstream metadata: {list(upstream_metadata.keys())}")
            context.log.info(f"🔍 Upstream metadata values: {upstream_metadata}")
            
            raise


@io_manager(
    required_resource_keys={"lakefs_fs", "lakefs_client"},
    config_schema={
        "default_repo": str,
        "default_branch": str,
    }
)
def dynamic_lakefs_io_manager(context):
    return DynamicLakeFSIOManager(
        fs=context.resources.lakefs_fs,
        client=context.resources.lakefs_client,
        default_repo=context.resource_config.get("default_repo", "spotify-repo"),
        default_branch=context.resource_config.get("default_branch", "main")
    )

