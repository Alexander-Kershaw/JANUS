from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @staticmethod
    def from_env() -> "DbConfig":

        # Reads DB config from environment

        return DbConfig(
            host=os.getenv("JANUS_DB_HOST", "localhost"),
            port=int(os.getenv("JANUS_DB_PORT", "5433")),
            database=os.getenv("JANUS_DB_NAME", "airflow"),
            user=os.getenv("JANUS_DB_USER", "janus"),
            password=os.getenv("JANUS_DB_PASSWORD", "janus_password"),
        )

    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


def make_engine(cfg: DbConfig | None = None) -> Engine:

    # Creates a SQLAlchemy engine for the local Postgres warehouse.

    cfg = cfg or DbConfig.from_env()
    return create_engine(cfg.sqlalchemy_url(), pool_pre_ping=True)