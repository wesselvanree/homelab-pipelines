from pydantic_settings import BaseSettings, SettingsConfigDict


class BybitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BYBIT_")

    base_url: str = "https://api-testnet.bybit.com"
