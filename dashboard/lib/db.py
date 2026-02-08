from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @staticmethod
    def from_env() -> "DbConfig":
        # Matched docker setup
        return DbConfig(
            host=os.getenv("JANUS_DB_HOST", "localhost"),
            port=int(os.getenv("JANUS_DB_PORT", "5433")),
            db=os.getenv("JANUS_DB_NAME", "airflow"),
            user=os.getenv("JANUS_DB_USER", "janus"),
            password=os.getenv("JANUS_DB_PASSWORD", "janus_password"),
        )


def make_engine(cfg: DbConfig | None = None) -> Engine:
    cfg = cfg or DbConfig.from_env()
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
    return create_engine(url, pool_pre_ping=True, future=True)


def read_sql_df(engine: Engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), engine, params=params)
