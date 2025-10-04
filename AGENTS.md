# Agent guides

## Build, test, and lint commands

- Build: `make build` or `CGO_ENABLED=0 go build`
- Run all tests: `make test` or `go test -v -cover`
- Run single test: `go test -v -run TestFunctionName`
- Coverage: `make coverage` (generates HTML report)
- Lint: `make staticcheck` or `staticcheck -f stylish`
- Format check: `gofmt -l .` (should return no files)

## Project structure

- Go 1.24+ codebase for managing Kafka with Confluent platform focus
- CLI commands: `cli_*.go` files (e.g., `cli_consumer.go`, `cli_topic.go`)
- IBM Sarama library for Kafka operations
- Main entry: `kafkalo.go`

## Code style

- Imports: Group stdlib, then external packages (use gofmt/goimports)
- Error handling: Return errors, use `log.Fatal()` for CLI-level failures
- Types: Use explicit types, avoid `interface{}` unless necessary
- Naming: camelCase for private, PascalCase for public; descriptive names
- Comments: Only for non-obvious logic (why, not what)
- Testing: Use `testify/assert` and `testify/require`, `testcontainers-go` for integration tests

## Critical rules

- Run tests after changes: `make test` - never accept breaking tests
- Prefer simple, readable code over clever solutions
- Use dependency injection for testability
- Follow Go security best practices (no secrets in logs/commits)

## Documentation

Documentation is using Sphinx with RST format in the `docs/` folder. To build the documentation, run:

```bash
make html
```
