import dagster as dg

comments_sentiment_analysis = dg.define_asset_job(
    name="comments_sentiment_analysis",
    selection=[
        dg.AssetKey("fetch_videos"),
        dg.AssetKey("fetch_comments"),
        dg.AssetKey("process_comments"),
        dg.AssetKey("comments_sentiment_inference")
    ]
)