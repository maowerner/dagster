import base64
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from io import StringIO
from typing import ContextManager, Iterator, Mapping, Optional

from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.external_execution.context import build_external_execution_context
from dagster._core.external_execution.resource import ExternalExecutionResource
from dagster._core.external_execution.task import (
    ExternalExecutionExtras,
    ExternalExecutionTask,
    ExternalTaskIOParams,
    ExternalTaskParams,
)
from dagster_externals import DAGSTER_EXTERNALS_ENV_KEYS, DagsterExternalError
from databricks.sdk.service import files
from pydantic import Field

from dagster_databricks.databricks import DatabricksClient, DatabricksError, DatabricksJobRunner


@dataclass
class DatabricksTaskParams(ExternalTaskParams):
    host: Optional[str]
    token: Optional[str]
    script_path: str
    git_url: str
    git_branch: str
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class DatabricksTaskIOParams(ExternalTaskIOParams):
    pass


# This has been manually uploaded to a test DBFS workspace.
# DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/d1c12eb4_a505_46d5_8717_9c24f8c6c9d1/dagster_externals-1!0+dev-py3-none-any.whl"
# DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/dagster_externals_current.whl"
DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/dagster_externals-1!0+dev-py3-none-any.whl"

TASK_KEY = "DUMMY_TASK_KEY"

CLUSTER_DEFAULTS = {
    "size": {"num_workers": 0},
    "spark_version": "12.2.x-scala2.12",
    "nodes": {"node_types": {"node_type_id": "i3.xlarge"}},
}


class DatabricksExecutionTask(ExternalExecutionTask[DatabricksTaskParams, DatabricksTaskIOParams]):
    def _launch(
        self,
        base_env: Mapping[str, str],
        params: DatabricksTaskParams,
        input_params: DatabricksTaskIOParams,
        output_params: DatabricksTaskIOParams,
    ) -> None:
        runner = DatabricksJobRunner(
            host=params.host,
            token=params.token,
        )

        config = {
            "install_default_libraries": False,
            "libraries": [
                {"whl": DAGSTER_EXTERNALS_WHL_PATH},
            ],
            # We need to set env vars so use a new cluster
            "cluster": {
                "new": {
                    **CLUSTER_DEFAULTS,
                    "spark_env_vars": {
                        **base_env,
                        **params.env,
                        **input_params.env,
                        **output_params.env,
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                    },
                }
            },
        }
        task = {
            "task_key": TASK_KEY,
            "spark_python_task": {
                "python_file": params.script_path,
                "source": "GIT",
            },
            "git_source": {
                "git_url": params.git_url,
                "git_branch": params.git_branch,
            },
        }

        run_id = runner.submit_run(config, task)
        try:
            runner.client.wait_for_run_to_complete(
                logger=self._context.log,
                databricks_run_id=run_id,
                poll_interval_sec=1,
                max_wait_time_sec=60 * 60,
            )
        except DatabricksError as e:
            raise DagsterExternalError(f"Error running Databricks job: {e}")

    # ########################
    # ##### IO CONTEXT MANAGERS
    # ########################

    def _input_context_manager(
        self, tempdir: str, params: DatabricksTaskParams
    ) -> ContextManager[DatabricksTaskIOParams]:
        client = DatabricksClient(params.host, params.token)
        return self._dbfs_input(tempdir, client)

    @contextmanager
    def _dbfs_input(
        self, tempdir: str, client: DatabricksClient
    ) -> Iterator[DatabricksTaskIOParams]:
        remote_path = "/test_dbfs_externals/input"
        dbfs = files.DbfsAPI(client.workspace_client.api_client)
        external_context = build_external_execution_context(self._context, self._extras)
        env = {DAGSTER_EXTERNALS_ENV_KEYS["input"]: remote_path}
        try:
            dbfs.mkdirs(os.path.dirname(remote_path))

            # contents = base64.b64encode(json.dumps(external_context))
            contents = base64.b64encode(json.dumps(external_context).encode("utf-8")).decode(
                "utf-8"
            )
            dbfs.put(remote_path, contents=contents)
            yield DatabricksTaskIOParams(env=env)
        finally:
            try:
                dbfs.delete(remote_path)
            except IOError:
                pass

    def _output_context_manager(
        self, tempdir: str, params: DatabricksTaskParams
    ) -> ContextManager[DatabricksTaskIOParams]:
        client = DatabricksClient(params.host, params.token)
        return self._dbfs_output(tempdir, client)

    @contextmanager
    def _dbfs_output(
        self, tempdir: str, client: DatabricksClient
    ) -> Iterator[DatabricksTaskIOParams]:
        remote_path = "/test_dbfs_externals/output"
        dbfs = files.DbfsAPI(client.workspace_client.api_client)
        env = {DAGSTER_EXTERNALS_ENV_KEYS["output"]: remote_path}
        try:
            yield DatabricksTaskIOParams(env=env)
            output = dbfs.read(remote_path).data
            for line in StringIO(output):
                notification = json.loads(line)
                self.handle_notification(notification)
        finally:
            try:
                dbfs.delete(remote_path)
            except IOError:
                pass


class DatabricksExecutionResource(ExternalExecutionResource):
    host: Optional[str] = Field(
        description="Databricks host, e.g. uksouth.azuredatabricks.com", default=None
    )
    token: Optional[str] = Field(description="Databricks access token", default=None)

    def run(
        self,
        context: OpExecutionContext,
        script_path: str,
        git_url: str,
        git_branch: str,
        *,
        extras: ExternalExecutionExtras,
    ) -> None:
        params = DatabricksTaskParams(
            host=self.host,
            token=self.token,
            script_path=script_path,
            git_url=git_url,
            git_branch=git_branch,
        )
        DatabricksExecutionTask(context=context, extras=extras).run(params)
