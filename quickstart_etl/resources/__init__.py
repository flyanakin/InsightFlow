from dagster import ConfigurableResource


class EnvResource(ConfigurableResource):
    tushare_token: str
    airtable_api_token: str
    airtable_base_id: str
    wechat_token: str
