from ib_insync import IB, Stock, Contract

def connect_ib(host: str, port: int, client_id: int) -> IB:
    ib = IB()
    ib.connect(host, port, clientId=client_id)
    return ib

def make_contract(symbol: str, exchange: str, currency: str, secType: str) -> Contract:
    if secType.upper() == "STK":
        return Stock(symbol, exchange, currency)
    raise ValueError(f"Unsupported secType: {secType}")
