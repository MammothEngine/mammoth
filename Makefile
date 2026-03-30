.PHONY: build test bench vet clean lint

BINARY=mammoth

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
