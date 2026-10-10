#!/bin/bash

# Services for the integration tests: RustFS (S3-compatible store), PostgreSQL and MySQL.
# Usage: ./scripts/test-services.sh [command]
# Commands:
#   up       Start the services and wait until they are healthy
#   down     Stop the services and delete their data
#   status   Show the services and their health
#   logs     Show the services' logs
#   test     Start the services, then run the service tests (extra args go to pytest)

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

PROJECT_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/.." && pwd))"
SERVICES_DIR="$PROJECT_ROOT/tests/integration-tests/services"
COMPOSE=(docker compose -f "$SERVICES_DIR/compose.yaml")

show_help() {
    echo "Forklift integration-test services (RustFS, PostgreSQL, MySQL)"
    echo ""
    echo "Usage: ./scripts/test-services.sh [command]"
    echo ""
    echo "Commands:"
    echo "  up        Start the services and wait until they are healthy"
    echo "  down      Stop the services and delete their data"
    echo "  status    Show the services and their health"
    echo "  logs      Show the services' logs"
    echo "  test      Start the services, then run tests/integration-tests/services"
    echo "            (extra arguments are passed to pytest)"
    echo "  --help    Show this help message"
    echo ""
    echo "The tests need the PostgreSQL and MariaDB (or MySQL) ODBC drivers, for example"
    echo "'apt-get install unixodbc odbc-postgresql odbc-mariadb' on Debian and Ubuntu."
}

case "${1:-}" in
    up)
        echo -e "${BLUE}Starting RustFS, PostgreSQL and MySQL...${NC}"
        "${COMPOSE[@]}" up -d --wait
        echo -e "${GREEN}✓ Services are healthy${NC}"
        echo -e "${BLUE}Run the tests with: ./scripts/test-services.sh test${NC}"
        ;;
    down)
        echo -e "${YELLOW}Stopping the services and deleting their data...${NC}"
        "${COMPOSE[@]}" down -v
        echo -e "${GREEN}✓ Services removed${NC}"
        ;;
    status)
        "${COMPOSE[@]}" ps
        ;;
    logs)
        "${COMPOSE[@]}" logs
        ;;
    test)
        shift
        "${COMPOSE[@]}" up -d --wait
        cd "$PROJECT_ROOT"
        FORKLIFT_TEST_SERVICES=1 python -m pytest tests/integration-tests/services --no-cov "$@"
        ;;
    --help|-h|help)
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command: ${1:-<none>}${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
