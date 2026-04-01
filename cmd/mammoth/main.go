package main

import (
	"fmt"
	"os"
	"runtime"
)

var version = "0.0.1" // Build-time variable, override with -ldflags

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Mammoth Engine v%s\n", version)
		fmt.Fprintln(os.Stderr, "Usage: mammoth <command> [options]")
		fmt.Fprintln(os.Stderr, "Commands: serve, repl, version, backup, restore, user, compact, validate, stats, bench, shard")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "repl":
		replCmd(os.Args[2:])
	case "version":
		fmt.Printf("Mammoth Engine v%s\n", version)
		fmt.Printf("Go: %s\n", runtime.Version())
		fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	case "shard":
		shardCmd(os.Args[2:])
	case "backup":
		backupCmd(os.Args[2:])
	case "restore":
		restoreCmd(os.Args[2:])
	case "user":
		userCmd(os.Args[2:])
	case "compact":
		compactCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	case "stats":
		statsCmd(os.Args[2:])
	/* case "bench":
		benchCmd(os.Args[2:]) */
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
