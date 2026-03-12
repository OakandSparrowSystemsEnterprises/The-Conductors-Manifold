"""
The Conductor's Manifold - FastAPI Backend

Main API server with REST endpoints and WebSocket support.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional
from datetime import datetime
import asyncio
import json
import os
import numpy as np

from backend.core.manifold_engine import (
    ManifoldEngine,
    MultiScaleAnalyzer,
    TimeScale,
    ManifoldMetrics
)
from backend.core.manifold_interpreter import (
    ManifoldInterpreter,
    ManifoldInterpretation
)
from backend.services.data_ingestion import (
    DataIngestionService,
    AlphaVantageDataFeed,
    BinanceDataFeed,
    BinanceUSDataFeed,
    CoinGeckoDataFeed,
    MarketData
)


# Initialize FastAPI app
app = FastAPI(
    title="The Conductor's Manifold API",
    description="Real-time geometric analysis of complex systems",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global services
manifold_engine = ManifoldEngine()
multiscale_analyzer = MultiScaleAnalyzer()
manifold_interpreter = ManifoldInterpreter()
data_service = DataIngestionService()

# WebSocket connection manager
class ConnectionManager:
    """Manages WebSocket connections for real-time updates"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Remove dead connections
                self.active_connections.remove(connection)


manager = ConnectionManager()


# Initialize data feeds on startup
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    # Register data feeds with API keys from environment
    alphavantage_key = os.getenv("ALPHAVANTAGE_API_KEY", "demo")
    data_service.register_feed(
        "alphavantage",
        AlphaVantageDataFeed(api_key=alphavantage_key)
    )
    data_service.register_feed(
        "binance",
        BinanceDataFeed()  # Global Binance (not available in USA)
    )
    data_service.register_feed(
        "binanceus",
        BinanceUSDataFeed()  # US-compliant, free, 2400 calls/min
    )
    data_service.register_feed(
        "coingecko",
        CoinGeckoDataFeed()  # Free, no API key needed
    )
    print("✨ The Conductor's Manifold API is online (Alpha Vantage + Binance.US + CoinGecko)")


# Helper functions
def _interpret_state(entropy: float, tension: float) -> str:
    """Interpret the overall manifold state"""
    if abs(tension) > 1.5:
        return "high_tension" if entropy > 5 else "compressed"
    elif entropy > 5:
        return "chaotic"
    elif abs(tension) < 0.5 and entropy < 3:
        return "stable"
    else:
        return "transitional"


def metrics_to_dict(metrics: ManifoldMetrics) -> dict:
    """Convert ManifoldMetrics to JSON-serializable dict"""
    # Calculate current values for UI display
    current_curvature = float(metrics.curvature[-1]) if len(metrics.curvature) > 0 else 0.0
    current_entropy = float(metrics.entropy)
    current_tension = float(metrics.tension[-1]) if len(metrics.tension) > 0 else 0.0

    # Calculate pylon strength from attractor strength (0-100 scale)
    # Higher attractor strength = stronger pylon integrity
    if metrics.attractors and len(metrics.attractors) > 0:
        avg_attractor_strength = sum(float(s) for _, s in metrics.attractors) / len(metrics.attractors)
        pylon_strength = min(100, max(0, int(avg_attractor_strength * 20)))  # Scale to 0-100
    else:
        pylon_strength = 0

    # Determine phase
    phase = _interpret_state(current_entropy, current_tension)

    return {
        "timestamp": metrics.timestamp.tolist(),
        "prices": metrics.prices.tolist(),
        "curvature": current_curvature,  # Single value for display
        "curvature_array": metrics.curvature.tolist(),  # Full array for charts
        "entropy": current_entropy,
        "local_entropy": metrics.local_entropy.tolist(),
        "singularities": metrics.singularities,
        "attractors": [
            {"price": float(p), "strength": float(s)}
            for p, s in metrics.attractors
        ],
        "ricci_flow": metrics.ricci_flow.tolist(),
        "tension": metrics.tension.tolist(),
        "tension_value": current_tension,  # Single value for display
        "timescale": metrics.timescale.value,
        "pylon_strength": pylon_strength,  # Computed field for UI
        "phase": phase.upper().replace("_", " ")  # Human readable phase
    }


# REST API Endpoints

@app.get("/")
async def root():
    """API health check"""
    return {
        "service": "The Conductor's Manifold",
        "status": "online",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/v1/analyze/{symbol}")
async def analyze_symbol(
    symbol: str,
    feed: str = "binance",
    interval: str = "1d",
    limit: int = 100,
    timescale: str = "daily"
):
    """
    Analyze a symbol and return manifold metrics.

    Args:
        symbol: Trading symbol (e.g., 'BTCUSDT' for Binance, 'AAPL' for stocks)
        feed: Data feed to use ('binance' or 'alphavantage')
        interval: Time interval for data
        limit: Number of data points to analyze
        timescale: Analysis timescale ('daily', 'weekly', 'monthly', 'intraday')

    Returns:
        Complete manifold analysis results
    """
    try:
        # Fetch market data
        market_data = await data_service.fetch_data(feed, symbol, interval, limit)

        if not market_data:
            raise HTTPException(status_code=404, detail="No data found for symbol")

        # Convert to numpy
        data_arrays = data_service.to_numpy(market_data)

        # Perform manifold analysis
        metrics = manifold_engine.analyze(
            prices=data_arrays['prices'],
            timestamps=data_arrays['timestamps'],
            timescale=TimeScale(timescale),
            volume=data_arrays['volume']
        )

        # Convert to JSON-serializable format
        result = metrics_to_dict(metrics)
        result['symbol'] = symbol
        result['feed'] = feed

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.get("/api/v1/multiscale/{symbol}")
async def analyze_multiscale(
    symbol: str,
    feed: str = "binance",
    interval: str = "1d",
    limit: int = 200
):
    """
    Perform multi-timeframe manifold analysis.

    Analyzes the symbol across monthly, weekly, daily, and intraday scales
    to reveal fractal consistency and cross-scale patterns.

    Returns:
        Dictionary of analyses for each timescale
    """
    try:
        # Fetch market data with more points for multi-scale
        market_data = await data_service.fetch_data(feed, symbol, interval, limit)

        if not market_data:
            raise HTTPException(status_code=404, detail="No data found for symbol")

        # Convert to numpy
        data_arrays = data_service.to_numpy(market_data)

        # Perform multi-scale analysis
        results = multiscale_analyzer.analyze_multiscale(
            prices=data_arrays['prices'],
            timestamps=data_arrays['timestamps']
        )

        # Map backend scales to frontend display names
        scale_mapping = {
            "intraday": "1H",
            "daily": "1D",
            "weekly": "1W",
            "monthly": "4H"  # Using 4H as intermediate scale
        }

        # Convert all metrics to JSON with enriched data
        response = {
            "symbol": symbol,
            "feed": feed,
            "scales": {}
        }

        for scale, metrics in results.items():
            scale_key = scale_mapping.get(scale.value, scale.value)

            # Get basic metrics
            scale_data = metrics_to_dict(metrics)

            # Add attractors if available
            if hasattr(metrics, 'attractors') and metrics.attractors:
                scale_data["attractors"] = [
                    {
                        "price": float(price),
                        "strength": float(strength)
                    }
                    for price, strength in metrics.attractors[:5]
                ]
            else:
                scale_data["attractors"] = []

            # Add singularities if available
            if hasattr(metrics, 'singularities') and metrics.singularities and hasattr(metrics, 'prices'):
                scale_data["singularities"] = [
                    {
                        "price": float(metrics.prices[idx]) if idx < len(metrics.prices) else 0.0,
                        "type": "extreme_tension"
                    }
                    for idx in metrics.singularities[:5]
                ]
            else:
                scale_data["singularities"] = []

            # Wrap in metrics object for frontend
            response["scales"][scale_key] = {
                "metrics": scale_data,
                "attractors": scale_data["attractors"],
                "singularities": scale_data["singularities"]
            }

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Multi-scale analysis failed: {str(e)}")


@app.get("/api/v1/attractors/{symbol}")
async def get_attractors(
    symbol: str,
    feed: str = "binance",
    limit: int = 100
):
    """
    Get current attractor zones for a symbol.

    Attractors are natural resting points where the manifold tends to stabilize.
    These can be used as support/resistance levels.

    Returns:
        List of attractor price levels with strength indicators
    """
    try:
        market_data = await data_service.fetch_data(feed, symbol, "1d", limit)
        data_arrays = data_service.to_numpy(market_data)

        attractors = manifold_engine.find_attractors(
            data_arrays['prices'],
            data_arrays['volume']
        )

        return {
            "symbol": symbol,
            "current_price": float(data_arrays['prices'][-1]),
            "attractors": [
                {
                    "price": float(p),
                    "strength": float(s),
                    "distance_pct": float((p - data_arrays['prices'][-1]) / data_arrays['prices'][-1] * 100)
                }
                for p, s in attractors
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/singularities/{symbol}")
async def get_singularities(
    symbol: str,
    feed: str = "binance",
    limit: int = 100
):
    """
    Detect recent singularities (extreme tension points).

    Singularities indicate where the manifold reached unsustainable
    extremes and corrections occurred or are imminent.

    Returns:
        List of recent singularity events with context
    """
    try:
        market_data = await data_service.fetch_data(feed, symbol, "1d", limit)
        data_arrays = data_service.to_numpy(market_data)

        metrics = manifold_engine.analyze(
            data_arrays['prices'],
            data_arrays['timestamps'],
            volume=data_arrays['volume']
        )

        # Get singularity details
        singularities = []
        for idx in metrics.singularities:
            if idx < len(market_data):
                singularities.append({
                    "timestamp": market_data[idx].timestamp.isoformat(),
                    "price": float(metrics.prices[idx]),
                    "curvature": float(metrics.curvature[idx]),
                    "tension": float(metrics.tension[idx]),
                    "entropy": float(metrics.local_entropy[idx])
                })

        return {
            "symbol": symbol,
            "singularities": singularities,
            "count": len(singularities)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/interpret/{symbol}")
async def interpret_manifold(
    symbol: str,
    feed: str = "binance",
    interval: str = "1d",
    limit: int = 100
):
    """
    **THE CONDUCTOR'S INTERPRETATION** - Proprietary Manifold Reading

    Returns the complete interpretation in the Conductor's language:
    - Phase diagnosis (impulse, singularity, Ricci flow, etc.)
    - Conductor perspective (macro flow)
    - Singer perspective (micro geometry)
    - Geometric descriptions (curvature, tension, entropy)
    - Elliott Wave context
    - Fibonacci attractor analysis
    - Human-readable market narrative

    This is the core proprietary methodology - interpreting the manifold
    as a living shape with musical composition dynamics.
    """
    try:
        # Fetch market data
        market_data = await data_service.fetch_data(feed, symbol, interval, limit)

        if not market_data:
            raise HTTPException(status_code=404, detail="No data found for symbol")

        # Convert to numpy
        data_arrays = data_service.to_numpy(market_data)

        # Analyze manifold
        metrics = manifold_engine.analyze(
            prices=data_arrays['prices'],
            timestamps=data_arrays['timestamps'],
            volume=data_arrays['volume']
        )

        # THE INTERPRETATION - Proprietary methodology
        interpretation = manifold_interpreter.interpret(metrics)

        # Convert to JSON-serializable format
        return {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "interpretation": {
                "phase": interpretation.current_phase.value,
                "phase_confidence": float(interpretation.phase_confidence),
                "conductor_reading": interpretation.conductor_reading.value,
                "singer_reading": interpretation.singer_reading.value,
                "curvature_state": interpretation.curvature_state,
                "tension_description": interpretation.tension_description,
                "entropy_state": interpretation.entropy_state,
                "wave_position": interpretation.wave_position,
                "nearest_attractor": {
                    "price": interpretation.nearest_attractor[0] if interpretation.nearest_attractor else None,
                    "description": interpretation.nearest_attractor[1] if interpretation.nearest_attractor else None
                } if interpretation.nearest_attractor else None,
                "attractor_pull_strength": float(interpretation.attractor_pull_strength),
                "market_narrative": interpretation.market_narrative,
                "tension_warning": interpretation.tension_warning
            },
            "raw_metrics": {
                "curvature": float(interpretation.curvature_value),
                "entropy": float(interpretation.entropy_value),
                "tension": float(interpretation.tension_value),
                "price": float(data_arrays['prices'][-1])
            }
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Interpretation failed: {str(e)}")


@app.get("/api/v1/pulse/{symbol}")
async def get_manifold_pulse(
    symbol: str,
    feed: str = "binance"
):
    """
    Get the current 'pulse' of the manifold - a quick health check.

    Returns key indicators:
    - Current entropy level (chaos vs stability)
    - Tension level
    - Distance to nearest attractor
    - Recent singularity count

    This is ideal for the "Continuous Readout" subscription tier.
    """
    try:
        market_data = await data_service.fetch_data(feed, symbol, "1d", 100)
        data_arrays = data_service.to_numpy(market_data)

        metrics = manifold_engine.analyze(
            data_arrays['prices'],
            data_arrays['timestamps'],
            volume=data_arrays['volume']
        )

        # Calculate pulse indicators
        current_price = float(data_arrays['prices'][-1])
        current_entropy = float(metrics.local_entropy[-1])
        current_tension = float(metrics.tension[-1])

        # Find nearest attractor
        nearest_attractor = min(
            metrics.attractors,
            key=lambda a: abs(a[0] - current_price)
        )
        distance_to_attractor = abs(nearest_attractor[0] - current_price)

        # Count recent singularities (last 20% of data)
        recent_threshold = int(len(metrics.singularities) * 0.8)
        recent_singularities = [s for s in metrics.singularities if s >= recent_threshold]

        return {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "pulse": {
                "current_price": current_price,
                "entropy": current_entropy,
                "entropy_level": "high" if current_entropy > 5 else "medium" if current_entropy > 3 else "low",
                "tension": current_tension,
                "tension_level": "high" if abs(current_tension) > 1.5 else "medium" if abs(current_tension) > 0.5 else "low",
                "nearest_attractor": {
                    "price": float(nearest_attractor[0]),
                    "distance": float(distance_to_attractor),
                    "distance_pct": float(distance_to_attractor / current_price * 100)
                },
                "recent_singularities": len(recent_singularities),
                "manifold_state": _interpret_state(current_entropy, current_tension)
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket endpoint for real-time updates
@app.websocket("/ws/realtime/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str, feed: str = "binance"):
    """
    WebSocket endpoint for real-time manifold updates.

    Streams continuous analysis updates as new data arrives.
    """
    await manager.connect(websocket)

    try:
        # Send initial state
        await websocket.send_json({
            "type": "connected",
            "symbol": symbol,
            "timestamp": datetime.now().isoformat()
        })

        # Real-time update loop
        while True:
            # Fetch latest data and analyze
            market_data = await data_service.fetch_data(feed, symbol, "1m", 100, use_cache=False)
            data_arrays = data_service.to_numpy(market_data)

            metrics = manifold_engine.analyze(
                data_arrays['prices'],
                data_arrays['timestamps'],
                volume=data_arrays['volume']
            )

            # Send update
            update = {
                "type": "update",
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "data": metrics_to_dict(metrics)
            }

            await websocket.send_json(update)

            # Wait before next update
            await asyncio.sleep(60)  # Update every minute

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
