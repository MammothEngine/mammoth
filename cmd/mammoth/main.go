package main

import (
	"fmt"
	"os"
)

const version = "0.7.0"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Mammoth Engine v%s\n", version)
		fmt.Fprintln(os.Stderr, "Usage: mammoth <command> [options]")
		fmt.Fprintln(os.Stderr, "Commands: serve, repl, version")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "repl":
		replCmd(os.Args[2:])
	case "version":
		fmt.Printf("Mammoth Engine v%s\n", version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
