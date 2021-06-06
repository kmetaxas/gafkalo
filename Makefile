.PHONY: test

all: build test
	
build:
	CGO_ENABLED=0 go build
test:
	go test -v -cover
coverage:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out
