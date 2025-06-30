import dagster as dg
import os
import pandas as pd

from ccfc_yt.infer import OpenRouterAsyncClient
from dags.assets.infer import utils, validate
from dags.resources import s3Resource
from datetime import datetime

monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(
    partitions_def=monthly_partitions,
    deps=[dg.AssetKey("process_comments")]
)
async def comments_sentiment_inference(context: dg.AssetExecutionContext, s3: s3Resource):
    """Run asynchronous API calls to OpenRouterAPI for Youtube comments"""
    partition_dt = datetime.strptime(context.partition_key, "%Y-%m-%d")
    context.log.info(f"Downloading comments data for {partition_dt} now")
    s3_key = f"s3://ccfcyoutube/processed/year={partition_dt.year}/month={partition_dt.month}/comments.jsonl"
    df = pd.read_json(s3_key, orient="records", lines=True)
    context.log.info(f"Filtering df for keywords to reduce noise")
    df = utils.filter_for_relevant_comments(df, "text_display_cleaned")
    comments = df["text_display_cleaned"].to_list()
    comment_ids = df["comment_id"].to_list()
    assert len(comments) == len(comment_ids), "comments and comment_ids lists are not the same length"
    system_prompt = (
        "You are a sentiment analysis assistant. "
        "Analyze the sentiment of the following text in relation to Coventry City FC. "
        "Reply only with a valid JSON object, on a single line, with two fields: "
        "`sentiment` (one of: positive, neutral, or negative) and `confidence` "
        "(a float between 0 and 1 representing your confidence). "
        "Do not include explanations, formatting, or markdown. "
        "Example: {\"sentiment\": \"neutral\", \"confidence\": 0.87}"
    )
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
    comment_messages_batch = [
        {
            "comment_id": comment_id,
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": comment
                }
            ]
        }
        for comment_id, comment in zip(comment_ids, comments)
    ]
    ai_client = OpenRouterAsyncClient(api_key=openrouter_api_key)
    context.log.info(f"Running async API calls now for {len(comment_messages_batch)} comments")
    results = await ai_client.chat_completion("openai/gpt-4.1-nano", messages_batch=comment_messages_batch)
    context.log.info("Async request finished")
    context.log.info("Processing and merging sentiment responses to df")
    sentiments = utils.process_llm_responses(results)
    sentiments_df = pd.DataFrame(sentiments)
    final_df = pd.merge(df, sentiments_df, on="comment_id")
    validate.schema.validate(final_df)
    final_df["year"] = final_df["published_at"].dt.year
    final_df["month"] = final_df["published_at"].dt.month
    final_df.to_parquet(
        "s3://ccfcyoutube/sentiment/",
        engine="pyarrow",
        index=False,
        partition_cols=["year", "month"]
    )