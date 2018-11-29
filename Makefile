GONAME=$(shell basename "$(PWD)")
GOFILES=$(wildcard src/*.go)
PID=go-$(GONAME).pid

build:
	@echo "Building .."
	@go build -o p2pServer ./server/
	@go build -o p2pClient client/client.go

start:
	@echo "Starting $(GONAME)...."
	@./p2pServer >>output.log 2>&1 & echo $$!> $(PID)

stop:
	@echo "Stopping server if running..."
	@-kill `[[ -f $(PID) ]] && cat $(PID)` 2>/dev/null || true

clean:
	go clean
	rm -f output.log
	rm -f p2pServer
	rm -f p2pClient
	rm -f *.pid

.PHONY: build start stop clean
