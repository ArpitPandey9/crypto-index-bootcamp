from src.coingecko import get_market_chart_daily

def test_pull_btc_minimal():
    df, source = get_market_chart_daily(days=30)
    assert len(df) >= 25
    assert {"date","price","market_cap","total_volume"} <= set(df.columns)
    assert source in {"coingecko", "coincap", "binance"}

