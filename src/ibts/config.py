from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List
import yaml

from .db import DBCfg

@dataclass(frozen=True)
class AppCfg:
    timeframe: str
    what_to_show: str
    use_rth: bool

@dataclass(frozen=True)
class IBCfg:
    host: str
    port: int
    client_id: int

@dataclass(frozen=True)
class SymbolCfg:
    symbol: str
    exchange: str
    currency: str
    secType: str
    start_time: str

@dataclass(frozen=True)
class Config:
    app: AppCfg
    ibkr: IBCfg
    db: DBCfg
    symbols: List[SymbolCfg]

def load_config(path: str = "config.yaml") -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f)

    app = AppCfg(
        timeframe=raw["app"]["timeframe"],
        what_to_show=raw["app"]["what_to_show"],
        use_rth=raw["app"]["use_rth"],
    )
    ibkr = IBCfg(**raw["ibkr"])
    db = DBCfg(**raw["db"])
    symbols = [SymbolCfg(**s) for s in raw["symbols"]]
    return Config(app=app, ibkr=ibkr, db=db, symbols=symbols)
