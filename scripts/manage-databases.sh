#!/bin/bash

# Database container management script for Forklift integration tests
# Usage: ./manage-databases.sh [command] [options]
# Commands:
#   start    Start database containers for integration tests
#   stop     Stop database containers (keeps data)
#   wipe     Stop and remove containers and volumes (destroys data)
#   status   Show status of database containers
#   logs     Show logs from database containers

set -e  # Exit on any error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default values
PROJECT_ROOT="$(dirname "$0")/.."
SQL_TESTS_DIR="$PROJECT_ROOT/tests/sql"

# Function to show help
show_help() {
    echo "Forklift Database Container Management"
    echo ""
    echo "Usage: ./manage-databases.sh [command] [options]"
    echo ""
    echo "Commands:"
    echo "  start      Start database containers for integration tests"
    echo "  stop       Stop database containers (preserves data)"
    echo "  wipe       Stop and remove containers and volumes (destroys data)"
    echo "  status     Show status of database containers"
    echo "  logs       Show logs from database containers"
    echo "  restart    Stop and start containers (preserves data)"
    echo "  --help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./manage-databases.sh start         # Start database containers"
    echo "  ./manage-databases.sh stop          # Stop containers but keep data"
    echo "  ./manage-databases.sh wipe          # Remove everything (fresh start)"
    echo "  ./manage-databases.sh status        # Check container status"
    echo "  ./manage-databases.sh logs          # View container logs"
    echo "  ./manage-databases.sh restart       # Restart containers"
}

# Function to check if docker-compose.yml exists
check_compose_file() {
    if [ ! -f "$SQL_TESTS_DIR/docker-compose.yml" ]; then
        echo -e "${RED}Error: docker-compose.yml not found in $SQL_TESTS_DIR${NC}"
        echo -e "${YELLOW}Make sure you're running this from the project root and that integration tests are set up.${NC}"
        exit 1
    fi
}

# Function to start containers
start_containers() {
    echo -e "${BLUE}Starting Database Testing Containers${NC}"
    echo -e "${BLUE}====================================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${YELLOW}Starting database containers...${NC}"
    if docker compose up -d; then
        echo ""
        echo -e "${GREEN}✓ Database containers started successfully${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh status' to check container health${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh stop' to stop containers${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh wipe' to remove containers and data${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to start database containers${NC}"
        exit 1
    fi
}

# Function to stop containers
stop_containers() {
    echo -e "${BLUE}Stopping Database Testing Containers${NC}"
    echo -e "${BLUE}====================================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${YELLOW}Stopping database containers (preserving data)...${NC}"
    if docker compose stop; then
        echo ""
        echo -e "${GREEN}✓ Database containers stopped successfully${NC}"
        echo -e "${BLUE}Data volumes have been preserved${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh start' to restart containers${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh wipe' to remove containers and data${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to stop database containers${NC}"
        exit 1
    fi
}

# Function to wipe containers and volumes
wipe_containers() {
    echo -e "${BLUE}Wiping Database Testing Containers${NC}"
    echo -e "${BLUE}==================================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${RED}⚠️  This will destroy all database data and containers!${NC}"
    echo -e "${YELLOW}Stopping containers and removing volumes...${NC}"
    if docker compose down -v; then
        echo ""
        echo -e "${GREEN}✓ Database containers and volumes removed successfully${NC}"
        echo -e "${BLUE}All database data has been destroyed${NC}"
        echo -e "${BLUE}Use './scripts/manage-databases.sh start' to create fresh containers${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to remove database containers${NC}"
        exit 1
    fi
}

# Function to show container status
show_status() {
    echo -e "${BLUE}Database Container Status${NC}"
    echo -e "${BLUE}========================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${YELLOW}Checking container status...${NC}"
    if docker compose ps; then
        echo ""
        echo -e "${GREEN}✓ Status check completed${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to check container status${NC}"
        exit 1
    fi
}

# Function to show container logs
show_logs() {
    echo -e "${BLUE}Database Container Logs${NC}"
    echo -e "${BLUE}======================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${YELLOW}Showing recent container logs...${NC}"
    if docker compose logs --tail=50; then
        echo ""
        echo -e "${GREEN}✓ Logs displayed${NC}"
        echo -e "${BLUE}Use 'docker compose logs -f' for live log streaming${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to show container logs${NC}"
        exit 1
    fi
}

# Function to restart containers
restart_containers() {
    echo -e "${BLUE}Restarting Database Testing Containers${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo ""

    check_compose_file
    cd "$SQL_TESTS_DIR"

    echo -e "${YELLOW}Restarting database containers (preserving data)...${NC}"
    if docker compose restart; then
        echo ""
        echo -e "${GREEN}✓ Database containers restarted successfully${NC}"
        echo -e "${BLUE}Data volumes have been preserved${NC}"
    else
        echo ""
        echo -e "${RED}✗ Failed to restart database containers${NC}"
        exit 1
    fi
}

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    echo -e "${RED}Error: No command specified${NC}"
    echo ""
    show_help
    exit 1
fi

case $1 in
    start)
        start_containers
        ;;
    stop)
        stop_containers
        ;;
    wipe)
        wipe_containers
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    restart)
        restart_containers
        ;;
    --help|help)
        show_help
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$1'${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
