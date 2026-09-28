"""Configuración centralizada, leída de variables de entorno (prefijo RC_) o de .env."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="RC_", extra="ignore")

    socrata_base_url: str = "https://www.datos.gov.co/resource"
    # SECOP II - Contratos Electrónicos
    dataset_id: str = "jbjy-vk9h"
    app_token: str | None = None

    page_size: int = Field(default=50_000, gt=0, le=50_000)
    request_timeout_s: float = 120.0
    max_retries: int = 5

    data_dir: Path = Path("data")

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw" / "secop_contratos"

    @property
    def endpoint(self) -> str:
        return f"{self.socrata_base_url}/{self.dataset_id}.json"
