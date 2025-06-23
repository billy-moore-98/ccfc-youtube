import dagster as dg

process_comments_job = dg.define_asset_job(
    name="process_comments_job",
    selection=[dg.AssetKey("process_comments")]
)