.PHONY: wasm test verify

TINYGO_IMAGE ?= tinygo/tinygo:0.39.0

wasm:
	docker run --rm -v "$(CURDIR):/src" -w /src $(TINYGO_IMAGE) \
		tinygo build -target=wasi -opt=z -no-debug \
		-o /src/wasm/build/gateway.wasm /src/wasm/gateway/main.go
	chmod 0644 wasm/build/gateway.wasm
	sha256sum wasm/build/gateway.wasm

test:
	go test ./...

verify: wasm
	go vet ./...
	go test ./...
	go test -race ./...
