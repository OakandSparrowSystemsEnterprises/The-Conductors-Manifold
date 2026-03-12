/**
 * Main Dashboard Component
 *
 * The Conductor's interface for observing the manifold in real-time.
 */

import React, { useState, useEffect } from 'react';
import ManifoldViewer3D from './ManifoldViewer3D';
import ManifoldPulse from './ManifoldPulse';
import MetricsPanel from './MetricsPanel';
import TimeframeSelector from './TimeframeSelector';
import MultiscaleView from './MultiscaleView';
import { useManifoldAPI } from '../services/api';
import './Dashboard.css';

const Dashboard = () => {
  // v1.3 - CRYPTO WORKING + MULTISCALE FIXED + 3D UPDATES
  const [symbol, setSymbol] = useState('BTCUSDT');
  const [feed, setFeed] = useState('binanceus');
  const [timescale, setTimescale] = useState('daily');
  const [manifoldData, setManifoldData] = useState(null);
  const [pulseData, setPulseData] = useState(null);
  const [multiscaleData, setMultiscaleData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [view, setView] = useState('3d'); // '3d', 'metrics', 'multiscale'

  const api = useManifoldAPI();

  // Fetch initial data - debounced to avoid rate limiting
  useEffect(() => {
    const timer = setTimeout(() => {
      loadManifoldData();
      loadPulseData();
    }, 800); // Wait 800ms after user stops typing

    return () => clearTimeout(timer);
  }, [symbol, feed, timescale]);

  // Clear multi-scale data when symbol or feed changes
  useEffect(() => {
    setMultiscaleData(null);
  }, [symbol, feed]);

  // Auto-refresh pulse every 30 seconds (paused while typing)
  useEffect(() => {
    // Only start auto-refresh after initial load (800ms debounce)
    const startDelay = setTimeout(() => {
      const interval = setInterval(loadPulseData, 30000);
      return () => clearInterval(interval);
    }, 1000);

    return () => clearTimeout(startDelay);
  }, [symbol, feed]);

  const loadManifoldData = async () => {
    setLoading(true);
    try {
      const data = await api.analyzeSymbol(symbol, feed, timescale);
      setManifoldData(data);
    } catch (error) {
      console.error('Failed to load manifold data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadPulseData = async () => {
    try {
      const data = await api.getManifoldPulse(symbol, feed);
      setPulseData(data);
    } catch (error) {
      console.error('Failed to load pulse data:', error);
    }
  };

  const loadMultiscaleData = async () => {
    setLoading(true);
    try {
      const data = await api.analyzeMultiscale(symbol, feed);
      setMultiscaleData(data);
    } catch (error) {
      console.error('Failed to load multiscale data:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="dashboard">
      {/* Header */}
      <header className="dashboard-header">
        <div className="header-content">
          <h1 className="title">The Conductor's Manifold [v1.3 LIVE]</h1>
          <p className="subtitle">Real-Time Geometric Interpretation · Stocks (Alpha Vantage) · Crypto (Binance.US)</p>
        </div>
      </header>

      {/* Controls */}
      <div className="controls-bar">
        <div className="control-group">
          <label>Symbol</label>
          <input
            type="text"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value.toUpperCase())}
            placeholder="BTCUSDT"
            className="input-field"
          />
        </div>

        <div className="control-group">
          <label>Data Feed</label>
          <select
            value={feed}
            onChange={(e) => setFeed(e.target.value)}
            className="select-field"
          >
            <option value="alphavantage">Alpha Vantage (US Stocks)</option>
            <option value="binanceus">Binance.US (Crypto - Free, High Limits)</option>
            <option value="coingecko">CoinGecko (Crypto - Rate Limited)</option>
          </select>
        </div>

        <div className="control-group">
          <label>Timescale</label>
          <TimeframeSelector
            current={timescale}
            onSelect={setTimescale}
          />
        </div>

        <button onClick={loadManifoldData} className="btn-primary" disabled={loading}>
          {loading ? 'Analyzing...' : 'Analyze'}
        </button>

        <button onClick={loadMultiscaleData} className="btn-secondary">
          Multi-Scale
        </button>
      </div>

      {/* View Selector */}
      <div className="view-selector">
        <button
          className={`view-btn ${view === '3d' ? 'active' : ''}`}
          onClick={() => setView('3d')}
        >
          3D Manifold
        </button>
        <button
          className={`view-btn ${view === 'metrics' ? 'active' : ''}`}
          onClick={() => setView('metrics')}
        >
          Metrics
        </button>
        <button
          className={`view-btn ${view === 'multiscale' ? 'active' : ''}`}
          onClick={() => setView('multiscale')}
        >
          Multi-Scale
        </button>
      </div>

      {/* Main Content */}
      <div className="main-content">
        {/* Pulse Monitor (Always Visible) */}
        {pulseData && (
          <div className="pulse-sidebar">
            <ManifoldPulse data={pulseData} />
          </div>
        )}

        {/* Primary View */}
        <div className="primary-view">
          {loading && (
            <div className="loading-overlay">
              <div className="loading-spinner"></div>
              <p>Analyzing the manifold...</p>
            </div>
          )}

          {!loading && view === '3d' && manifoldData && (
            <div className="view-container">
              <ManifoldViewer3D
                manifoldData={manifoldData}
                width={1000}
                height={700}
              />
            </div>
          )}

          {!loading && view === 'metrics' && manifoldData && (
            <div className="view-container">
              <MetricsPanel data={manifoldData} />
            </div>
          )}

          {!loading && view === 'multiscale' && multiscaleData && (
            <div className="view-container">
              <MultiscaleView data={multiscaleData} />
            </div>
          )}

          {!loading && !manifoldData && (
            <div className="empty-state">
              <h2>Enter a symbol and press Analyze to begin</h2>
              <p>Observe the market as a living geometric manifold</p>
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <footer className="dashboard-footer">
        <p>© 2025 Joshua Johosky. All Rights Reserved.</p>
        <p className="disclaimer">For authorized use only. See LICENSE.md for terms.</p>
      </footer>
    </div>
  );
};

export default Dashboard;
