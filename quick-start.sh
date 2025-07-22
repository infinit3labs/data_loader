#!/bin/bash

# Quick Start Script for Databricks Data Loader Docker Environment
# This script provides a simple interface to get started with testing

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
echo "=========================================="
echo "  Databricks Data Loader - Quick Start  "
echo "=========================================="
echo -e "${NC}"

# Function to print status
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

print_success "Docker is available and running"

# Show menu
echo ""
echo "What would you like to do?"
echo ""
echo "1) Run complete test suite (recommended for first time)"
echo "2) Run unit tests only"
echo "3) Run integration tests only"
echo "4) Generate mock data only"
echo "5) Start interactive development environment"
echo "6) Start Jupyter notebook for data exploration"
echo "7) Build Docker images only"
echo "8) Clean up all containers and volumes"
echo "9) Show test results"
echo "0) Exit"
echo ""

read -p "Enter your choice (0-9): " choice

case $choice in
    1)
        print_status "Running complete test suite..."
        ./docker/scripts/run_tests.sh --full
        ;;
    2)
        print_status "Running unit tests..."
        ./docker/scripts/run_tests.sh --unit
        ;;
    3)
        print_status "Running integration tests..."
        ./docker/scripts/run_tests.sh --integration
        ;;
    4)
        print_status "Generating mock data..."
        docker-compose up --no-deps data-generator
        print_success "Mock data generated in data volume"
        ;;
    5)
        print_status "Starting interactive development environment..."
        print_status "Building images if needed..."
        docker-compose build
        
        print_status "Generating test data..."
        docker-compose up --no-deps data-generator
        
        print_status "Starting main container..."
        docker-compose up -d data-loader
        
        print_success "Environment started!"
        echo ""
        echo "Available commands:"
        echo "  docker-compose exec data-loader bash  # Enter container"
        echo "  docker-compose logs -f data-loader    # View logs"
        echo "  http://localhost:4040                 # Spark UI (when running)"
        echo ""
        echo "Press Enter to open a shell in the container, or Ctrl+C to exit"
        read
        docker-compose exec data-loader bash
        ;;
    6)
        print_status "Starting Jupyter notebook environment..."
        docker-compose up -d jupyter data-generator
        sleep 3
        print_success "Jupyter started!"
        echo ""
        echo "Open your browser to: http://localhost:8888"
        echo "No password required for development environment"
        echo ""
        echo "Press Enter to continue or Ctrl+C to stop services"
        read
        docker-compose down
        ;;
    7)
        print_status "Building Docker images..."
        docker-compose build --no-cache
        print_success "Images built successfully"
        ;;
    8)
        print_warning "This will remove all containers, volumes, and test data!"
        read -p "Are you sure? (y/N): " confirm
        if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
            ./docker/scripts/run_tests.sh --cleanup
            print_success "Cleanup completed"
        else
            print_status "Cleanup cancelled"
        fi
        ;;
    9)
        print_status "Showing test results..."
        echo ""
        if [ -d "./test-results" ]; then
            echo "Unit Test Results:"
            echo "=================="
            if [ -f "./test-results/junit.xml" ]; then
                echo "JUnit results available: ./test-results/junit.xml"
            fi
            if [ -d "./test-results/coverage" ]; then
                echo "Coverage report: ./test-results/coverage/index.html"
            fi
            echo ""
        fi
        
        if [ -d "./integration-test-results" ]; then
            echo "Integration Test Results:"
            echo "========================"
            if [ -f "./integration-test-results/test_summary.txt" ]; then
                cat "./integration-test-results/test_summary.txt"
            else
                echo "Integration test results available in: ./integration-test-results/"
            fi
            echo ""
        fi
        
        if [ ! -d "./test-results" ] && [ ! -d "./integration-test-results" ]; then
            print_warning "No test results found. Run tests first with option 1, 2, or 3."
        fi
        ;;
    0)
        print_status "Goodbye!"
        exit 0
        ;;
    *)
        print_error "Invalid choice. Please run the script again."
        exit 1
        ;;
esac

print_success "Operation completed!"
