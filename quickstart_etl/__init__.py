from dagster import (
    Definitions,
    load_assets_from_package_module,
    EnvVar,
    define_asset_job,
    AssetSelection
)
from quickstart_etl.resources import EnvResource
from . import assets

push_job = define_asset_job(
    name='push_job',
)

defs = Definitions(
    assets=load_assets_from_package_module(assets),
    jobs=[push_job],
    resources={
        "env": EnvResource(
            tushare_token=EnvVar("TUSHARE_TOKEN"),
            airtable_api_token=EnvVar("AIRTABLE_API_TOKEN"),
            airtable_base_id=EnvVar("AIRTABLE_BASE_ID"),
            wechat_token=EnvVar("WECOM_BOT_TOKEN_KIKYO"),
        ),
    },
)