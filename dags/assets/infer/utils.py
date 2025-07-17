import json
import pandas as pd

from ccfc_yt.infer import OpenRouterAsyncClient
from dags.assets.infer import validate
from datetime import datetime
from typing import Dict, List

SYSTEM_PROMPT = (
    "You are a sentiment analysis assistant. "
    "Analyze the sentiment of the following text in relation to Coventry City FC. "
    "Reply only with a valid JSON object, on a single line, with two fields: "
    "`sentiment` (one of: positive, neutral, or negative) and `confidence` "
    "(a float between 0 and 1 representing your confidence). "
    "Do not include explanations, formatting, or markdown. "
    "Example: {\"sentiment\": \"neutral\", \"confidence\": 0.87}"
)

def filter_for_relevant_comments(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Filter a comments DataFrame for keywords before running sentiment inference"""
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
        "eccles",
        "robins",
        "mark"
    ]
    df = df[
        df[column].str.lower().apply(
            lambda text: any(kw in text for kw in keywords)
        )
    ]
    return df

def load_comments_from_s3(partition_dt: datetime) -> pd.DataFrame:
    s3_key = f"s3://ccfcyoutube/silver/year={partition_dt.year}/month={partition_dt.month}/comments.jsonl"
    df = pd.read_json(s3_key, orient="records", lines=True)
    df_filtered = filter_for_relevant_comments(df, "text_display_cleaned")
    return df_filtered

def prepare_comment_messages(df: pd.DataFrame) -> List[Dict]:
    return [
        {
            "comment_id": row["comment_id"],
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": row["text_display_cleaned"]}
            ]
        }
        for _, row in df.iterrows()
    ]

async def run_sentiment_inference(comment_messages_batch: List[Dict], api_key: str) -> list[dict]:
    ai_client = OpenRouterAsyncClient(api_key=api_key)
    return await ai_client.chat_completion("openai/gpt-4.1-nano", messages_batch=comment_messages_batch)

def process_llm_responses(responses: List[Dict]) -> List[Dict]:
    sentiments = []
    for response in responses:
        try:
            if not response["error"]:
                raw_content: str = response["response"]["choices"][0]["message"]["content"]
                cleaned = raw_content.strip()
                parsed = json.loads(cleaned)
                sentiments.append({
                    "comment_id": response["comment_id"],
                    "sentiment": parsed["sentiment"],
                    "confidence": parsed["confidence"]
                })
        except json.decoder.JSONDecodeError as e:
            print(f"JSONDecodeError for comment: {response["comment_id"]}, string: {raw_content}")
    return sentiments

def merge_and_validate_results(original_df: pd.DataFrame, results: list[dict]) -> pd.DataFrame:
    sentiments_df = pd.DataFrame(process_llm_responses(results))
    final_df = pd.merge(original_df, sentiments_df, on="comment_id")
    validate.schema.validate(final_df)
    final_df["year"] = final_df["published_at"].dt.year
    final_df["month"] = final_df["published_at"].dt.month
    return final_df

def write_output_to_s3(df: pd.DataFrame):
    df.to_parquet(
        "s3://ccfcyoutube/gold/",
        engine="pyarrow",
        index=False,
        partition_cols=["year", "month"]
    )