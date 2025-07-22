#!/bin/bash

# Docker Test Runner - Comprehensive testing suite for Databricks Data Loader
# This script orchestrates the entire testing process in Docker containers

set -e  # Exit on any error

echo "=================================================="
echo "Databricks Data Loader - Docker Test Suite"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Check if Docker is running
check_docker() {
    print_status "Checking Docker availability..."
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Build Docker images
build_images() {
    print_status "Building Docker images..."
    docker-compose build --no-cache
    if [ $? -eq 0 ]; then
        print_success "Docker images built successfully"
    else
        print_error "Failed to build Docker images"
        exit 1
    fi
}

# Clean up previous test runs
cleanup() {
    print_status "Cleaning up previous test runs..."
    docker-compose down -v > /dev/null 2>&1 || true
    docker volume prune -f > /dev/null 2>&1 || true
    print_success "Cleanup completed"
}

# Generate mock data
generate_data() {
    print_status "Generating mock test data..."
    docker-compose up --no-deps data-generator
    
    if [ $? -eq 0 ]; then
        print_success "Mock data generated successfully"
    else
        print_error "Failed to generate mock data"
        exit 1
    fi
}

# Run unit tests
run_unit_tests() {
    print_status "Running unit tests..."
    docker-compose up --no-deps test-runner
    
    # Copy test results to host
    docker cp data-loader-tests:/app/test-results ./test-results 2>/dev/null || true
    
    if [ $? -eq 0 ]; then
        print_success "Unit tests completed"
    else
        print_warning "Unit tests may have failed - check test results"
    fi
}

# Run integration tests
run_integration_tests() {
    print_status "Running integration tests..."
    docker-compose up --no-deps integration-tests
    
    # Copy integration test results
    docker cp integration-tests:/app/test-results ./integration-test-results 2>/dev/null || true
    
    if [ $? -eq 0 ]; then
        print_success "Integration tests completed"
    else
        print_warning "Integration tests may have failed - check test results"
    fi
}

# Run manual tests
run_manual_tests() {
    print_status "Running manual data loading tests..."
    
    # Test standard mode
    print_status "Testing standard mode..."
    docker-compose run --rm data-loader python3 -m data_loader.main run \
        --config /app/docker/configs/docker_test_config.json \
        --tables "transactions" \
        --dry-run
    
    # Test cluster mode
    print_status "Testing cluster mode..."
    docker-compose run --rm data-loader python3 -m data_loader.main run-cluster \
        --config /app/docker/configs/cluster_test_config.json \
        --tables "customers" \
        --dry-run
    
    print_success "Manual tests completed"
}

# Start interactive environment
start_interactive() {
    print_status "Starting interactive test environment..."
    print_status "You can now use the following commands:"
    echo "  - docker-compose exec data-loader bash (enter the container)"
    echo "  - docker-compose logs -f data-loader (view logs)"
    echo "  - http://localhost:8888 (Jupyter notebook)"
    echo "  - http://localhost:4040 (Spark UI when running)"
    echo ""
    print_status "Starting services..."
    docker-compose up -d data-loader jupyter
    
    print_success "Interactive environment started!"
    print_status "Press Ctrl+C to stop all services"
    
    # Wait for interrupt
    trap 'docker-compose down' INT
    while true; do
        sleep 1
    done
}

# Generate test report
generate_report() {
    print_status "Generating test report..."
    
    REPORT_FILE="test_report_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$REPORT_FILE" << EOF
# Databricks Data Loader - Test Report

**Generated:** $(date)
**Environment:** Docker Testing Environment

## Test Summary

### Unit Tests
$(if [ -d "./test-results" ]; then
    echo "- Location: ./test-results/"
    echo "- Coverage Report: ./test-results/coverage/index.html"
    if [ -f "./test-results/junit.xml" ]; then
        echo "- JUnit Results: Available"
    fi
else
    echo "- Status: Not run or results not available"
fi)

### Integration Tests
$(if [ -d "./integration-test-results" ]; then
    echo "- Location: ./integration-test-results/"
    if [ -f "./integration-test-results/test_summary.txt" ]; then
        echo "- Summary: Available"
        echo ""
        echo "\`\`\`"
        cat "./integration-test-results/test_summary.txt" 2>/dev/null || echo "Could not read summary"
        echo "\`\`\`"
    fi
else
    echo "- Status: Not run or results not available"
fi)

## Docker Environment

### Services
- **data-loader**: Main application container
- **test-runner**: Unit test execution
- **data-generator**: Mock data generation
- **integration-tests**: End-to-end testing
- **jupyter**: Interactive analysis environment

### Volumes
- **data-volume**: Persistent test data storage
- **logs-volume**: Application logs
- **test-results-volume**: Test outputs and reports

## Usage Examples

\`\`\`bash
# Build and run all tests
./docker/scripts/run_tests.sh --full

# Run only unit tests
./docker/scripts/run_tests.sh --unit

# Start interactive environment
./docker/scripts/run_tests.sh --interactive

# Clean up everything
./docker/scripts/run_tests.sh --cleanup
\`\`\`

EOF

    print_success "Test report generated: $REPORT_FILE"
}

# Main execution
main() {
    case "${1:-}" in
        --unit)
            check_docker
            cleanup
            build_images
            generate_data
            run_unit_tests
            generate_report
            ;;
        --integration)
            check_docker
            cleanup
            build_images
            generate_data
            run_integration_tests
            generate_report
            ;;
        --manual)
            check_docker
            cleanup
            build_images
            generate_data
            run_manual_tests
            ;;
        --full)
            check_docker
            cleanup
            build_images
            generate_data
            run_unit_tests
            run_integration_tests
            run_manual_tests
            generate_report
            ;;
        --interactive)
            check_docker
            cleanup
            build_images
            generate_data
            start_interactive
            ;;
        --cleanup)
            cleanup
            print_success "Cleanup completed"
            ;;
        --build)
            check_docker
            build_images
            ;;
        --help)
            echo "Databricks Data Loader - Docker Test Runner"
            echo ""
            echo "Usage: $0 [OPTION]"
            echo ""
            echo "Options:"
            echo "  --unit         Run unit tests only"
            echo "  --integration  Run integration tests only"
            echo "  --manual       Run manual testing scenarios"
            echo "  --full         Run complete test suite (default)"
            echo "  --interactive  Start interactive testing environment"
            echo "  --build        Build Docker images only"
            echo "  --cleanup      Clean up containers and volumes"
            echo "  --help         Show this help message"
            echo ""
            exit 0
            ;;
        *)
            print_status "Running full test suite (use --help for options)"
            check_docker
            cleanup
            build_images
            generate_data
            run_unit_tests
            run_integration_tests
            run_manual_tests
            generate_report
            ;;
    esac
}

# Run main function with all arguments
main "$@"
