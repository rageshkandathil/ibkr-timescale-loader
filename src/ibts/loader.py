from __future__ import annotations

from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
from typing import Optional
from ib_insync import BarDataList


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _bar_to_row(symbol: str, timeframe: str, bar) -> dict:
    ts = bar.date
    if isinstance(ts, str):
        ts = isoparse(ts)
    ts = _to_utc(ts)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "ts": ts,
        "open": float(bar.open) if bar.open is not None else None,
        "high": float(bar.high) if bar.high is not None else None,
        "low": float(bar.low) if bar.low is not None else None,
        "close": float(bar.close) if bar.close is not None else None,
        "volume": float(bar.volume) if bar.volume is not None else None,
        "wap": float(getattr(bar, "wap", None)) if getattr(bar, "wap", None) is not None else None,
        "bar_count": int(getattr(bar, "barCount", None)) if getattr(bar, "barCount", None) is not None else None,
        "source": "IBKR",
    }

def compute_effective_start(config_start_iso: str, last_ts: Optional[datetime]) -> datetime:
    """
    Start from config start time, but if DB already has data, continue from (last_ts + 1 bar).
    Assumes 1-min bars for the +1 minute. (We’ll generalize later.)
    """
    config_start = _to_utc(isoparse(config_start_iso))
    if last_ts is None:
        return config_start
    return max(config_start, _to_utc(last_ts) + timedelta(minutes=1))

def backfill_symbol(
    ib,
    db,
    symbol: str,
    contract,
    timeframe: str,
    what_to_show: str,
    use_rth: bool,
    start_utc: datetime,
) -> None:
    """
    Correct IBKR backfill pattern: pull backwards from 'now' in small durations
    until we reach start_utc. This avoids IB result limits.
    """
    end = datetime.now(timezone.utc)
    duration_str = "1 D"  # safe for 1-min bars; can try "2 D" later

    safety = 0
    while end > start_utc:
        safety += 1
        if safety > 5000:
            raise RuntimeError("Backfill safety stop triggered. Check looping logic.")

        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end,
            durationStr=duration_str,
            barSizeSetting=timeframe,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=2,
            keepUpToDate=False,
        )

        if not bars:
            end = end - timedelta(days=1)
            continue

        rows = [_bar_to_row(symbol, timeframe, b) for b in bars]
        db.insert_bars(rows)

        # Move end backward to just before earliest returned bar
        earliest_ts = rows[0]["ts"]
        end = earliest_ts - timedelta(minutes=1)

def stream_symbol(
    ib,
    db,
    symbol: str,
    contract,
    timeframe: str,
    what_to_show: str,
    use_rth: bool,
) -> BarDataList:
    """
    Subscribe to live-updating historical bars.
    On each update, insert the latest bar (duplicates ignored by DB PK).
    """
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr="2 D",          # small window is enough; it will keep updating
        barSizeSetting=timeframe,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=True,
    )

    def on_update(updated_bars: BarDataList, has_new_bar: bool) -> None:
        if not updated_bars:
            return
        latest = updated_bars[-1]
        row = _bar_to_row(symbol, timeframe, latest)
        db.insert_bars([row])

    bars.updateEvent += on_update
    return bars
