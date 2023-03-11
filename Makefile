.PHONY: test build staticcheck coverage

all: build
	
build:
	CGO_ENABLED=0 go build
test: build
	go test -v -cover

staticcheck:
	staticcheck -f stylish
coverage: build
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out
