#!/bin/bash

# Automated test script for Redis Collections
# This script:
# 1. Starts Redis Cluster
# 2. Waits for initialization
# 3. Runs tests
# 4. Cleans up (stops cluster)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Redis Collections Test Runner ===${NC}\n"

# Function to cleanup on exit
cleanup() {
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}Tests completed successfully!${NC}"
    else
        echo -e "\n${RED}Tests failed!${NC}"
    fi

    echo -e "${YELLOW}Stopping Redis Cluster...${NC}"
    docker-compose -f infra/docker-compose.yml down -v
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup function to run on script exit
trap cleanup EXIT

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: docker-compose is not installed${NC}"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Start Redis Cluster
echo -e "${YELLOW}Starting Redis Cluster...${NC}"
docker-compose -f infra/docker-compose.yml up -d

# Wait for cluster initialization
echo -e "${YELLOW}Waiting for Redis Cluster initialization (30 seconds)...${NC}"
sleep 30

# Verify cluster is ready
echo -e "${YELLOW}Verifying cluster status...${NC}"
CLUSTER_STATE=$(docker exec infra-redis-node-1-1 redis-cli -p 7000 cluster info 2>/dev/null | grep cluster_state | cut -d: -f2 | tr -d '\r')

if [ "$CLUSTER_STATE" != "ok" ]; then
    echo -e "${RED}Error: Redis Cluster failed to initialize${NC}"
    echo -e "${YELLOW}Check logs: docker-compose -f infra/docker-compose.yml logs redis-cluster-init${NC}"
    exit 1
fi

echo -e "${GREEN}Redis Cluster is ready!${NC}\n"

# Show cluster info
echo -e "${YELLOW}Cluster Info:${NC}"
docker exec infra-redis-node-1-1 redis-cli -p 7000 cluster nodes | head -3

# Run tests
echo -e "\n${YELLOW}Running tests...${NC}\n"
mvn test

# Run demo application to show Map/List usage against the live cluster
echo -e "\n${YELLOW}Running demo application (mvn exec:java)...${NC}\n"
mvn exec:java

# Cleanup will be called automatically via trap
