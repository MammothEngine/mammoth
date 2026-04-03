package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mammothengine/mammoth/pkg/engine"
)

func compactCmd(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	force := fs.Bool("force", false, "force compaction regardless of thresholds")
	fs.Parse(args)

	opts := engine.DefaultOptions(*dataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	stats := eng.Stats()
	fmt.Printf("Before compaction:\n")
	fmt.Printf("  Memtables: %d (%d bytes)\n", stats.MemtableCount, stats.MemtableSizeBytes)
	fmt.Printf("  SSTables:  %d (%d bytes)\n", stats.SSTableCount, stats.SSTableTotalBytes)

	if *force || stats.MemtableCount > 2 || stats.SSTableCount > 4 {
		fmt.Println("Running compaction...")
		if err := eng.MaybeCompact(); err != nil {
			fmt.Fprintf(os.Stderr, "Compaction error: %v\n", err)
			os.Exit(1)
		}
		// Flush to ensure memtables are written
		if err := eng.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Flush error: %v\n", err)
		}
	}

	stats = eng.Stats()
	fmt.Printf("After compaction:\n")
	fmt.Printf("  Memtables: %d (%d bytes)\n", stats.MemtableCount, stats.MemtableSizeBytes)
	fmt.Printf("  SSTables:  %d (%d bytes)\n", stats.SSTableCount, stats.SSTableTotalBytes)
	fmt.Println("Compaction complete.")
}
