.PHONY: test

all: build
	
build:
	CGO_ENABLED=0 go build
test:
	go test -v -cover

staticcheck:
	staticcheck -f stylish
coverage:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out
