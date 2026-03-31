.PHONY: build test bench vet clean lint release docker docker-push dist

BINARY=mammoth
VERSION?=0.8.0

build:
	go build -o $(BINARY) ./cmd/mammoth

test:
	go test -race -count=1 ./...

bench:
	go test -bench=. -benchmem ./...

vet:
	go vet ./...

lint: vet

clean:
	rm -f $(BINARY)
	rm -rf testdata/

# Cross-compile release binaries
release:
	@echo "Building release v$(VERSION)..."
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-linux-amd64 ./cmd/mammoth
	GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-linux-arm64 ./cmd/mammoth
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-darwin-amd64 ./cmd/mammoth
	GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-darwin-arm64 ./cmd/mammoth
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-windows-amd64.exe ./cmd/mammoth
	GOOS=windows GOARCH=arm64 go build -ldflags="-s -w" -o dist/mammoth-$(VERSION)-windows-arm64.exe ./cmd/mammoth
	@echo "Generating checksums..."
	cd dist && sha256sum mammoth-$(VERSION)-* > checksums-$(VERSION).txt
	@echo "Release binaries in dist/"

# Docker
docker:
	docker build -t mammoth-engine:$(VERSION) .
	docker tag mammoth-engine:$(VERSION) mammoth-engine:latest

docker-push:
	docker push mammoth-engine:$(VERSION)
	docker push mammoth-engine:latest

# Distribution package
dist: release
	@echo "Distribution complete."
