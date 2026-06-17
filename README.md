# Trade Hub

Trade Hub is an algorithmic trading engine designed to automate, monitor, and execute option strategies. It supports real-time multi-leg strategy management, streaming market data feed, and broker APIs.

## Features

- **Broker Integration**: Built-in support for major brokers (e.g., Angel One).
- **Multi-Leg Strategies**: Live monitoring and calculation of multi-leg option spreads (e.g. Bull/Bear spreads, Straddles, Strangles).
- **Real-Time Data Feed**: Live streaming and updating of Last Traded Price (LTP), bid/ask prices, and order statuses.
- **Web UI Dashboard**: Clean and modern HTML template views for managing active positions and strategies.

---

## Project Structure

```
├── templates/                 # Web templates for UI dashboard
│   ├── index.html             # Main strategies dashboard UI
│   └── business_guide.html    # User manual and strategy reference
├── algo.py                    # Main algorithmic trading engine script
├── strategies.json            # Persistence layer for strategy definitions
├── client_config.example.json # Placeholder template for API credentials
└── README.md                  # Project documentation
```

---

## Getting Started

### 1. Prerequisites

Make sure you have Python 3.8+ installed along with any required packages (such as `jugaad-trader` or relevant API client libraries).

### 2. Configuration Setup

Copy the example configuration to create your live configuration:

```bash
cp client_config.example.json client_config.json
```

Open `client_config.json` and fill in your broker details and API keys:

```json
{
    "client_name": "YOUR_NAME",
    "client_email": "YOUR_EMAIL",
    "client_mobile": "YOUR_MOBILE",
    "selected_broker": "ANGEL_ONE",
    "credentials": {
        "api_key": "YOUR_API_KEY",
        "client_code": "YOUR_CLIENT_CODE",
        "password": "YOUR_PASSWORD"
    },
    "mode": "REAL",
    "registered_at": ""
}
```

*Note: `client_config.json` and `tokens.json` are excluded from Git to prevent accidental credential leakage.*

### 3. Running the Engine

Start the trading engine by running:

```bash
python algo.py
```
