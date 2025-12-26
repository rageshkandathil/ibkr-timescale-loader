from datetime import timezone
from dateutil.parser import isoparse

from ibts.config import load_config
from ibts.db import DB, build_dsn
from ibts.ib import connect_ib, make_contract
from ibts.loader import backfill_symbol, stream_symbol


def main():
    cfg = load_config("config.yaml")

    db = DB(build_dsn(cfg.db))
    db.connect()

    ib = connect_ib(cfg.ibkr.host, cfg.ibkr.port, cfg.ibkr.client_id)

    streams = []
    try:
        for s in cfg.symbols:
            contract = make_contract(s.symbol, s.exchange, s.currency, s.secType)
            ib.qualifyContracts(contract)

            # Backfill from config start time (IB will limit 1-min depth; that's OK)
            start_utc = isoparse(s.start_time).astimezone(timezone.utc)

            backfill_symbol(
                ib=ib,
                db=db,
                symbol=s.symbol,
                contract=contract,
                timeframe=cfg.app.timeframe,
                what_to_show=cfg.app.what_to_show,
                use_rth=cfg.app.use_rth,
                start_utc=start_utc,
            )
            print(f"Backfill complete for {s.symbol}")

            # Live stream (append new bars as they form)
            streams.append(
                stream_symbol(
                    ib=ib,
                    db=db,
                    symbol=s.symbol,
                    contract=contract,
                    timeframe=cfg.app.timeframe,
                    what_to_show=cfg.app.what_to_show,
                    use_rth=cfg.app.use_rth,
                )
            )
            print(f"Live streaming started for {s.symbol}")

        print("Streaming... press Ctrl+C to stop.")
        ib.run()

    except KeyboardInterrupt:
        print("Stopping...")

    finally:
        db.close()
        ib.disconnect()


if __name__ == "__main__":
    main()
