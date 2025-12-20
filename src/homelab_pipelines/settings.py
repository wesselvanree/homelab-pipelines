import dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

dotenv.load_dotenv()


class BybitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BYBIT_")

    base_url: str
    """The base url for the HTTP api (e.g. `https://api-testnet.bybit.com` for testnet)"""


class ModelSettings(BaseSettings):
    n_days_history_train: int = 256
    n_days_history_train_min: int = 128
