import dagster as dg
import json
import os

from dags.assets.infer.utils import (
    catch_failed_requests,
    load_comments_from_s3,
    prepare_comment_messages,
    run_sentiment_inference,
    merge_and_validate_results,
    write_output_to_s3
)
from dags.resources import s3Resource
from datetime import datetime, timezone

monthly_partitions = dg.MonthlyPartitionsDefinition("2023-08-01")

@dg.asset(
    partitions_def=monthly_partitions,
    deps=[dg.AssetKey("process_comments")]
)
async def comments_sentiment_inference(context: dg.AssetExecutionContext, s3: s3Resource):
    """Run asynchronous API calls to OpenRouterAPI for Youtube comments"""
    partition_dt = datetime.strptime(context.partition_key, "%Y-%m-%d")
    context.log.info(f"Starting sentiment inference for {partition_dt.date()}")

    df = load_comments_from_s3(partition_dt)
    context.log.info(f"Loaded {len(df)} filtered comments")

    comment_messages = prepare_comment_messages(df)
    context.log.info(f"Prepared {len(comment_messages)} comment messages for inference")

    api_key = os.getenv("OPENROUTER_API_KEY")
    results = await run_sentiment_inference(comment_messages, api_key)
    context.log.info("Inference complete")

    success, errors = catch_failed_requests(results)
    if errors:
        context.log.warning(f"{len(errors)} requests failed during inference")
        context.log.info("Uploading unsuccessful requests to S3 for review")
        run_time = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"failed_requests/year={partition_dt.year}/month={partition_dt.month}/failed_requests_{run_time}.json"
        s3._client.put_object(
            Bucket="ccfcyoutube",
            Body=json.dumps(errors),
            Key=key,
            ContentType="application/json"
        )

    final_df = merge_and_validate_results(df, success)
    context.log.info("Results merged and validated")

    write_output_to_s3(final_df)
    context.log.info("Output written to S3")