# Build the fix_tx_hash tool
build-fix-tx-hash:
	go build -o bin/fix_tx_hash ./cmd/tools/fix_tx_hash.go

# Run the fix_tx_hash tool
fix-tx-hash: build-fix-tx-hash
	./bin/fix_tx_hash $(ARGS) 