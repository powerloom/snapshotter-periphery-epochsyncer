# snapshotter-periphery-epochsyncer

A service that monitors and synchronizes epoch events from the blockchain, ensuring block and transaction data is cached before forwarding events to downstream workers.

## Components

The service consists of two main components:

1. **EpochSyncer Service**
   - Monitors blockchain for epoch release events
   - Checks Redis cache for block and transaction data availability
   - Uses Dramatiq to send messages to downstream workers when data is ready
   - Configurable RPC endpoints and rate limiting
   - Detailed logging and monitoring

2. **Rate Limiter Service**
   - Provides rate limiting for RPC calls
   - REST API endpoint for rate limit management
   - Health check monitoring
   - Configurable default rate limits

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Blockchain RPC │◄────┤  EpochSyncer    │◄────┤  Rate Limiter   │
│                 │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Redis Cache    │◄────┤  Cache Checker  │     │  Dramatiq       │
│                 │     │  (Background)   │     │  Workers        │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐
│                 │
│  Downstream     │
│  Workers        │
│                 │
└─────────────────┘
```

## Features

- **Event Detection**: Monitors blockchain for epoch release events
- **Cache Verification**: 
  - Checks Redis for block data availability
  - Verifies transaction data in Redis hash tables
  - Background tasks for continuous cache checking
- **Message Queue**: Uses Dramatiq for reliable message delivery
- **Rate Limiting**: Integrated rate limiting for RPC calls
- **Configurable**: Extensive environment variable configuration
- **Logging**: Comprehensive logging with rotation and retention

## Environment Variables

See `env.example` for all configurable environment variables.

## Running the Service

```bash
docker-compose up -d
```

## Health Checks

- Rate Limiter: `http://localhost:8000`
- EpochSyncer: Monitored through logs and Dramatiq queue status
