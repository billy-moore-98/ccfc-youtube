import dagster as dg
import io
import json
import os
import pandas as pd

from ccfc_yt.infer import OpenRouterAsyncClient
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
    s3_key = f"s3://bmooreawsbucket/processed/year={partition_dt.year}/month={partition_dt.month}/comments.jsonl"
    df = pd.read_json(s3_key, orient="records", lines=True)
    keywords = [
        "coventry",
        "pusb",
        "ccfc",
        "sky blues",
        "sky blue",
        "cbs",
        "arena",
        "lampard",
        "wright",
        "sheaf",
        "sakamoto",
        "bidwell",
        "eccles"
    ]
    context.log.info(f"Filtering df for keywords to reduce noise")
    df = df[
        df["text_display_cleaned"].str.lower().apply(
            lambda text: any(kw in text for kw in keywords)
        )
    ].head(100)
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
    sentiments = []
    context.log.info(f"Example result: {results[0]}")
    for result in results:
        if not result["error"]:
            try:
                raw_content = result["response"]["choices"][0]["message"]["content"]
                cleaned = raw_content.strip()

                # Optionally validate the JSON with a regex or check
                parsed = json.loads(cleaned)

                sentiments.append({
                    "comment_id": result["comment_id"],
                    "sentiment": parsed["sentiment"],
                    "confidence": parsed["confidence"]
                })
            except json.decoder.JSONDecodeError as e:
                context.log.error("JSON decode error occurred")
                context.log.error(f"Raw content was: {repr(raw_content)}")
                context.log.error(f"Result set is: {result}")
                raise
    sentiments_df = pd.DataFrame(sentiments)
    final = pd.merge(df, sentiments_df, on="comment_id")
    buffer = io.StringIO()
    final.to_csv(buffer, index=False)
    buffer.seek(0)
    context.log.info("Uploading df to s3 now")
    s3._client.put_object(Bucket="bmooreawsbucket", Key=f"sentiment/{partition_dt.strftime("%Y%m")}_sentiment.csv", Body=buffer.getvalue())
    context.log.info("Asset materialised")