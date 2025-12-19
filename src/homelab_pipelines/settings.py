from pydantic_settings import BaseSettings, SettingsConfigDict


class BybitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BYBIT_")

    base_url: str = "https://api-testnet.bybit.com"


class ModelSettings(BaseSettings):
    n_days_history_train: int = 256
    n_days_history_train_min: int = 128
