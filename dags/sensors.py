import dagster as dg

from dags.assets.process import ProcessConfig
from dags.jobs import process_comments_job
from datetime import datetime

# here we set up an asset sensor that requests a run of the process_comments_job
# whenever a fetch_comments partition successfully materialises
# we also provide the partition as a ProcessConfig obj to the process_comments asset
@dg.asset_sensor(asset_key=dg.AssetKey("fetch_comments"), job=process_comments_job)
def fetch_comments_sensor(context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry):
    partition_key = asset_event.dagster_event.partition
    return dg.RunRequest(
        run_key=f"process_comments_{partition_key}",
        config=dg.RunConfig(
            {
                'process_comments': ProcessConfig(
                    partition_dt=partition_key
                )
            }
        )
    )