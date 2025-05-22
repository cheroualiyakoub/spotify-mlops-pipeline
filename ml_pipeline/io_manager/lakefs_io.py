from dagster import IOManager, io_manager
from lakefs_spec import LakeFSFileSystem
import os
import pandas as pd

class LakeFSIOManager(IOManager):
    def __init__(self, repo: str, branch: str):
        self.fs = LakeFSFileSystem() 
        self.repo = repo
        self.branch = branch

    def _get_path(self, context):
        return f"{self.repo}/{self.branch}/{'/'.join(context.asset_key.path)}"

    def handle_output(self, context, obj):
        lakefs_path = self._get_path(context)
        with self.fs.open(f"lakefs://{lakefs_path}", "wb") as f:
            if isinstance(obj, pd.DataFrame):
                obj.to_parquet(f)
            else:
                f.write(obj)

        context.add_output_metadata({
            "lakefs_path": f"lakefs://{lakefs_path}",
            "commit_id": self.fs.client.commit(self.repo, self.branch).id
        })

    def load_input(self, context):
        upstream_path = context.upstream_output.metadata["lakefs_path"]
        with self.fs.open(upstream_path, "rb") as f:
            return pd.read_parquet(f) if upstream_path.endswith(".parquet") else f.read()

@io_manager(required_resource_keys={"lakefs_fs", "lakefs_client"})
def lakefs_io_manager(context):
    return LakeFSIOManager(
        fs=context.resources.lakefs_fs,
        client=context.resources.lakefs_client,
        repo="spotify-repo"
    )