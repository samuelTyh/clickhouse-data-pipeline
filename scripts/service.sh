#!/bin/bash

# Script to manage Docker Compose services
# Usage: ./service.sh start|stop|restart|status|logs [service_name]

DOCKER_COMPOSE_FILE="docker-compose.yaml"

function start_services() {
    echo "Starting Docker Compose services..."
    docker-compose -f $DOCKER_COMPOSE_FILE up -d
    echo "Services started."
}

function stop_services() {
    echo "Stopping Docker Compose services..."
    docker-compose -f $DOCKER_COMPOSE_FILE down -v --rmi local
    echo "Services stopped."
}

function restart_services() {
    echo "Restarting Docker Compose services..."
    stop_services
    start_services
    echo "Services restarted."
}

function status_services() {
    echo "Checking status of Docker Compose services..."
    docker-compose -f $DOCKER_COMPOSE_FILE ps
}

function check_service_log() {
    if [ -z "$1" ]; then
        echo "Please provide a service name to check logs."
        echo "Usage: $0 logs <service_name>, available services are:"
        echo "etl, postgres, clickhouse, seeder"
        exit 1
    fi
    echo "Checking logs for service: $1"
    docker-compose -f $DOCKER_COMPOSE_FILE logs "$1"
}

# Check for the command argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 start|stop|restart|status|logs [service_name]"
    exit 1
fi

# Execute the appropriate function based on the argument
case $1 in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        status_services
        ;;
    logs)
        check_service_log "$2"
        ;;
    *)
        echo "Invalid command. Usage: $0 start|stop|restart|status|logs [service_name]"
        exit 1
        ;;
esac