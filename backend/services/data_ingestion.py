"""
Data Ingestion Service

Handles real-time market data acquisition from various sources.
"""

import asyncio
import aiohttp
import numpy as np
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
import json


@dataclass
class MarketData:
    """Container for market data points"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class DataFeed:
    """Base class for market data feeds"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self):
        """Initialize connection"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def disconnect(self):
        """Close connection"""
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_historical(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100
    ) -> List[MarketData]:
        """Fetch historical data"""
        raise NotImplementedError

    async def subscribe_realtime(
        self,
        symbol: str,
        callback: Callable[[MarketData], None]
    ):
        """Subscribe to real-time updates"""
        raise NotImplementedError


class AlphaVantageDataFeed(DataFeed):
    """Alpha Vantage API data feed for stocks"""

    BASE_URL = "https://www.alphavantage.co/query"

    async def fetch_historical(
        self,
        symbol: str,
        interval: str = "daily",
        limit: int = 100
    ) -> List[MarketData]:
        """
        Fetch historical stock data from Alpha Vantage.

        Args:
            symbol: Stock ticker (e.g., 'AAPL')
            interval: Time interval ('daily', 'weekly', 'monthly')
            limit: Number of data points to fetch

        Returns:
            List of MarketData objects
        """
        await self.connect()

        # Map interval to Alpha Vantage function
        function_map = {
            "daily": "TIME_SERIES_DAILY",
            "weekly": "TIME_SERIES_WEEKLY",
            "monthly": "TIME_SERIES_MONTHLY",
            "intraday": "TIME_SERIES_INTRADAY"
        }

        params = {
            "function": function_map.get(interval, "TIME_SERIES_DAILY"),
            "symbol": symbol,
            "apikey": self.api_key or "demo",
            "outputsize": "full" if limit > 100 else "compact"
        }

        if interval == "intraday":
            params["interval"] = "5min"

        async with self.session.get(self.BASE_URL, params=params) as response:
            data = await response.json()

            # Parse the response
            time_series_key = [k for k in data.keys() if "Time Series" in k]
            if not time_series_key:
                raise ValueError(f"No time series data found for {symbol}")

            time_series = data[time_series_key[0]]

            market_data = []
            for timestamp_str, values in list(time_series.items())[:limit]:
                timestamp = datetime.strptime(
                    timestamp_str,
                    "%Y-%m-%d %H:%M:%S" if interval == "intraday" else "%Y-%m-%d"
                )

                market_data.append(MarketData(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(values.get("1. open", 0)),
                    high=float(values.get("2. high", 0)),
                    low=float(values.get("3. low", 0)),
                    close=float(values.get("4. close", 0)),
                    volume=float(values.get("5. volume", 0))
                ))

            # Sort by timestamp (oldest first)
            market_data.sort(key=lambda x: x.timestamp)
            return market_data


class BinanceDataFeed(DataFeed):
    """Binance Global API data feed for crypto (not available in USA)"""

    BASE_URL = "https://api.binance.com/api/v3"
    WS_URL = "wss://stream.binance.com:9443/ws"

    async def fetch_historical(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100
    ) -> List[MarketData]:
        """
        Fetch historical crypto data from Binance.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Kline interval ('1m', '5m', '1h', '1d', etc.)
            limit: Number of candles to fetch

        Returns:
            List of MarketData objects
        """
        await self.connect()

        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, 1000)  # Binance max limit
        }

        async with self.session.get(
            f"{self.BASE_URL}/klines",
            params=params
        ) as response:
            data = await response.json()

            market_data = []
            for candle in data:
                timestamp = datetime.fromtimestamp(candle[0] / 1000)

                market_data.append(MarketData(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5])
                ))

            return market_data

    async def subscribe_realtime(
        self,
        symbol: str,
        callback: Callable[[MarketData], None]
    ):
        """
        Subscribe to real-time kline updates via WebSocket.

        Args:
            symbol: Trading pair
            callback: Function to call with new data
        """
        import websockets

        stream = f"{symbol.lower()}@kline_1m"
        uri = f"{self.WS_URL}/{stream}"

        async with websockets.connect(uri) as websocket:
            async for message in websocket:
                data = json.loads(message)
                kline = data.get("k")

                if kline:
                    market_data = MarketData(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(kline["t"] / 1000),
                        open=float(kline["o"]),
                        high=float(kline["h"]),
                        low=float(kline["l"]),
                        close=float(kline["c"]),
                        volume=float(kline["v"])
                    )

                    callback(market_data)


class BinanceUSDataFeed(DataFeed):
    """Binance.US API data feed for crypto (US-compliant, high rate limits)"""

    BASE_URL = "https://api.binance.us/api/v3"
    WS_URL = "wss://stream.binance.us:9443/ws"

    async def fetch_historical(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100
    ) -> List[MarketData]:
        """
        Fetch historical crypto data from Binance.US.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT', 'XRPUSDT')
            interval: Kline interval ('1m', '5m', '1h', '1d', etc.)
            limit: Number of candles to fetch

        Returns:
            List of MarketData objects
        """
        await self.connect()

        # Binance.US uses USDT pairs
        if not symbol.endswith("USDT") and not symbol.endswith("USD"):
            symbol = f"{symbol}USDT"

        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, 1000)  # Binance.US max limit
        }

        try:
            async with self.session.get(
                f"{self.BASE_URL}/klines",
                params=params
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    raise ValueError(f"Binance.US API error: {text}")

                data = await response.json()

                market_data = []
                for candle in data:
                    timestamp = datetime.fromtimestamp(candle[0] / 1000)

                    market_data.append(MarketData(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=float(candle[1]),
                        high=float(candle[2]),
                        low=float(candle[3]),
                        close=float(candle[4]),
                        volume=float(candle[5])
                    ))

                return market_data

        except Exception as e:
            raise ValueError(f"Failed to fetch data from Binance.US: {str(e)}")

    async def subscribe_realtime(
        self,
        symbol: str,
        callback: Callable[[MarketData], None]
    ):
        """
        Subscribe to real-time kline updates via WebSocket.

        Args:
            symbol: Trading pair
            callback: Function to call with new data
        """
        import websockets

        stream = f"{symbol.lower()}@kline_1m"
        uri = f"{self.WS_URL}/{stream}"

        async with websockets.connect(uri) as websocket:
            async for message in websocket:
                data = json.loads(message)
                kline = data.get("k")

                if kline:
                    market_data = MarketData(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(kline["t"] / 1000),
                        open=float(kline["o"]),
                        high=float(kline["h"]),
                        low=float(kline["l"]),
                        close=float(kline["c"]),
                        volume=float(kline["v"])
                    )

                    callback(market_data)


class CoinGeckoDataFeed(DataFeed):
    """
    CoinGecko API feed for crypto data (US-compliant, free)
    """
    BASE_URL = "https://api.coingecko.com/api/v3"

    async def fetch_historical(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100
    ) -> List[MarketData]:
        """Fetch historical crypto data from CoinGecko"""
        await self.connect()

        # Map common symbols to CoinGecko IDs
        symbol_map = {
            "BTCUSD": "bitcoin",
            "ETHUSD": "ethereum",
            "XRPUSD": "ripple",
            "XRP": "ripple",
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "SOLUSD": "solana",
            "SOL": "solana",
            "ADAUSD": "cardano",
            "ADA": "cardano"
        }

        coin_id = symbol_map.get(symbol.upper(), symbol.lower())

        # CoinGecko only supports daily data
        days = min(limit, 365)  # Max 365 days for free tier

        url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily"
        }

        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise ValueError(f"CoinGecko API error: {text}")

                data = await response.json()

                # Parse CoinGecko response
                prices = data.get("prices", [])
                volumes = data.get("total_volumes", [])

                if not prices:
                    raise ValueError(f"No time series data found for {symbol}")

                market_data_list = []
                for i, (timestamp_ms, price) in enumerate(prices):
                    volume = volumes[i][1] if i < len(volumes) else 0

                    market_data_list.append(MarketData(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(timestamp_ms / 1000),
                        open=price,  # CoinGecko doesn't provide OHLC for free
                        high=price,
                        low=price,
                        close=price,
                        volume=volume
                    ))

                return market_data_list[-limit:]

        except Exception as e:
            raise ValueError(f"Failed to fetch data from CoinGecko: {str(e)}")


class DataIngestionService:
    """
    Main service for managing data feeds and caching.
    """

    def __init__(self):
        self.feeds: Dict[str, DataFeed] = {}
        self.cache: Dict[str, List[MarketData]] = {}

    def register_feed(self, name: str, feed: DataFeed):
        """Register a data feed"""
        self.feeds[name] = feed

    async def fetch_data(
        self,
        feed_name: str,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
        use_cache: bool = True
    ) -> List[MarketData]:
        """
        Fetch market data, using cache if available.

        Args:
            feed_name: Name of registered feed
            symbol: Trading symbol
            interval: Time interval
            limit: Number of data points
            use_cache: Whether to use cached data

        Returns:
            List of MarketData
        """
        cache_key = f"{feed_name}:{symbol}:{interval}"

        # Check cache
        if use_cache and cache_key in self.cache:
            cached = self.cache[cache_key]
            if len(cached) >= limit:
                return cached[-limit:]

        # Fetch from feed
        if feed_name not in self.feeds:
            raise ValueError(f"Feed '{feed_name}' not registered")

        feed = self.feeds[feed_name]
        data = await feed.fetch_historical(symbol, interval, limit)

        # Update cache
        self.cache[cache_key] = data

        return data

    def to_numpy(self, data: List[MarketData]) -> Dict[str, np.ndarray]:
        """
        Convert MarketData list to numpy arrays.

        Returns:
            Dictionary with 'timestamps', 'prices', 'volume' arrays
        """
        return {
            'timestamps': np.array([d.timestamp.timestamp() for d in data]),
            'prices': np.array([d.close for d in data]),
            'opens': np.array([d.open for d in data]),
            'highs': np.array([d.high for d in data]),
            'lows': np.array([d.low for d in data]),
            'volume': np.array([d.volume for d in data])
        }


# Example usage
async def example_usage():
    """Example of how to use the data ingestion service"""
    import os

    service = DataIngestionService()

    # Register feeds (get API key from environment)
    alphavantage_key = os.getenv("ALPHAVANTAGE_API_KEY", "demo")
    service.register_feed("alphavantage", AlphaVantageDataFeed(api_key=alphavantage_key))
    service.register_feed("binance", BinanceDataFeed())

    # Fetch stock data
    stock_data = await service.fetch_data("alphavantage", "AAPL", "daily", 100)
    print(f"Fetched {len(stock_data)} stock data points")

    # Fetch crypto data
    crypto_data = await service.fetch_data("binance", "BTCUSDT", "1d", 100)
    print(f"Fetched {len(crypto_data)} crypto data points")

    # Convert to numpy for analysis
    numpy_data = service.to_numpy(crypto_data)
    print(f"Price range: {numpy_data['prices'].min():.2f} - {numpy_data['prices'].max():.2f}")


if __name__ == "__main__":
    asyncio.run(example_usage())
