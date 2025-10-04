=======
Testing
=======

Overview
--------

The gafkalo test suite includes unit tests for core functionality and validation logic.

Test Structure
--------------

Unit Tests
~~~~~~~~~~

Located in various ``*_test.go`` files:

- ``auth_test.go`` - Authentication and security tests (mTLS, SCRAM, TLS)
- ``cli_test.go`` - CLI argument parsing tests
- ``config_test.go`` - Configuration parsing tests
- ``lint_topic_test.go`` - Topic linting tests
- ``report_test.go`` - Reporting functionality tests
- ``result_test.go`` - Result handling tests
- ``topics_test.go`` - Topic creation validation tests

Running Tests
-------------

All Tests
~~~~~~~~~

.. code-block:: bash

   make test
   # or
   go test -v -cover

Specific Test
~~~~~~~~~~~~~

.. code-block:: bash

   go test -v -run TestFunctionName

Topic Tests
~~~~~~~~~~~

.. code-block:: bash

   # Run all topic-related tests
   go test -v -run TestTopic

   # Run specific test
   go test -v -run TestTopicCreateValidation

Coverage Report
~~~~~~~~~~~~~~~

.. code-block:: bash

   make coverage

This generates an HTML coverage report.

Test Coverage
-------------

Topic Creation Tests
~~~~~~~~~~~~~~~~~~~~

**TestTopicCreateValidation**

- Purpose: Tests parameter validation for topic creation
- Verifies:
  - Valid partitions and replication factor pass validation
  - Zero partitions are rejected
  - Zero replication factor is rejected
  - Negative partitions are rejected
- Duration: < 1 second
- Type: Unit test (no Kafka required)

**TestTopicListItemSorting**

- Purpose: Tests topic list sorting behavior
- Verifies: Topics can be properly structured and sorted
- Duration: < 1 second
- Type: Unit test (no Kafka required)

Testing Best Practices
-----------------------

Writing New Tests
~~~~~~~~~~~~~~~~~

1. Use table-driven tests for parameter validation
2. Use meaningful test names that describe what is being tested
3. Add assertions for all critical behaviors
4. Keep tests isolated (no shared state)
5. Prefer unit tests over integration tests for speed

Test Organization
~~~~~~~~~~~~~~~~~

.. code-block:: go

   func TestFeatureName(t *testing.T) {
       // Arrange: Setup test data
       input := setupTestData()
       
       // Act: Perform the action
       result := functionUnderTest(input)
       
       // Assert: Verify results
       assert.Equal(t, expected, result)
   }

Table-Driven Tests
~~~~~~~~~~~~~~~~~~

.. code-block:: go

   func TestValidation(t *testing.T) {
       tests := []struct {
           name          string
           input         string
           expectedError bool
       }{
           {"valid input", "valid", false},
           {"invalid input", "", true},
       }
       
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               err := validate(tt.input)
               if tt.expectedError {
                   assert.Error(t, err)
               } else {
                   assert.NoError(t, err)
               }
           })
       }
   }

Integration Testing
-------------------

For end-to-end testing with real Kafka clusters:

Using Docker Compose
~~~~~~~~~~~~~~~~~~~~

Test environments are available in ``testdata/compose/``:

.. code-block:: bash

   # Start Kafka cluster
   cd testdata/compose/allinone
   docker-compose up -d
   
   # Run CLI commands
   ./gafkalo topic create -n test --config test-config.yaml
   
   # Verify
   ./gafkalo topic list --config test-config.yaml

Manual Testing
~~~~~~~~~~~~~~

1. Start a local Kafka cluster (via Docker, Confluent Platform, etc.)
2. Create a configuration file pointing to your cluster
3. Run CLI commands to test functionality
4. Verify results using describe/list commands

CI/CD Integration
-----------------

GitHub Actions
~~~~~~~~~~~~~~

The project uses GitHub Actions for CI. See ``.github/workflows/go.yml``.

Workflow includes:

- Go version: 1.24+
- Test execution: ``make test``
- Lint check: ``make staticcheck``
- Build verification: ``make build``

Local Development
~~~~~~~~~~~~~~~~~

Prerequisites:

- Go 1.24 or higher
- Make (for Makefile targets)

Running tests locally:

.. code-block:: bash

   # Install dependencies
   go mod download
   
   # Run all tests
   make test
   
   # Run with coverage
   make coverage
   
   # Run linter
   make staticcheck
   
   # Format check
   gofmt -l .

Troubleshooting
---------------

Test Failures
~~~~~~~~~~~~~

If tests fail:

1. Check Go version: ``go version`` (requires 1.24+)
2. Update dependencies: ``go mod download``
3. Check for compilation errors: ``go build``
4. Run specific failing test: ``go test -v -run TestName``
5. Check test output for specific error messages

Build Issues
~~~~~~~~~~~~

.. code-block:: bash

   # Clean and rebuild
   go clean
   go build
   
   # Check for module issues
   go mod tidy
   go mod verify

Coverage Goals
--------------

- Unit test coverage: Target > 80%
- Critical paths: 100% coverage
- Edge cases: Comprehensive coverage
- Error handling: All error paths tested

Future Enhancements
-------------------

Potential test improvements:

- Add benchmark tests for performance-critical operations
- Expand table-driven tests for more parameter combinations
- Add integration test suite with testcontainers (when networking issues resolved)
- Add performance regression tests
- Add property-based testing for complex logic
