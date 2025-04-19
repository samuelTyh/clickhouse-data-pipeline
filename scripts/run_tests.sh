#!/bin/bash

# Run unit tests
if [ -z "$1" ]; then
    echo "No argument provided. Running unit tests only."
    echo "Running unit tests..."
    python -m pytest -xvs -m "unit"
else
    echo "Argument provided: $1"
fi

# Run integration tests if specified
if [ "$1" == "--integration" ]; then
    echo "Running integration tests..."
    docker-compose up -d postgres clickhouse
    sleep 10
    python -m pytest -xvs -m "integration"
    docker-compose down -v
fi

# Run schema tests if specified
if [ "$1" == "--schema" ]; then
    echo "Running all tests..."
    python -m pytest -xvs -m "schema"
fi
