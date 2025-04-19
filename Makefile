# Variables
SERVICE_SCRIPT = ./scripts/service.sh
TEST_SCRIPT = ./scripts/run_tests.sh

# Targets
.PHONY: start stop restart status logs test

# Docker Compose Service Management
start:
	@$(SERVICE_SCRIPT) start

stop:
	@$(SERVICE_SCRIPT) stop

restart:
	@$(SERVICE_SCRIPT) restart

status:
	@$(SERVICE_SCRIPT) status

logs:
	@if [ -z "$(service)" ]; then \
		echo "Please specify a service name using 'make logs service=<service_name>'"; \
	else \
	$(SERVICE_SCRIPT) logs $(service); \
	fi

# Test Management
test:
	@if [ -z "$(type)" ]; then \
        echo "No test type specified. Running unit tests by default..."; \
        $(TEST_SCRIPT); \
    else \
        echo "Running $(type) tests..."; \
        $(TEST_SCRIPT) --$$(echo $(type)); \
    fi