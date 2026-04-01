package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func shardCmd(args []string) {
	fs := flag.NewFlagSet("shard", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: mammoth shard <subcommand> [options]")
		fmt.Fprintln(os.Stderr, "Subcommands:")
		fmt.Fprintln(os.Stderr, "  add        Add a shard to the cluster")
		fmt.Fprintln(os.Stderr, "  remove     Remove a shard from the cluster")
		fmt.Fprintln(os.Stderr, "  list       List all shards")
		fmt.Fprintln(os.Stderr, "  status     Show sharding status")
		fmt.Fprintln(os.Stderr, "  enable     Enable sharding for a database")
		fmt.Fprintln(os.Stderr, "  shard-key  Configure shard key for a collection")
		fmt.Fprintln(os.Stderr, "  split      Manually split a chunk")
		fmt.Fprintln(os.Stderr, "  move       Move a chunk to a different shard")
		fmt.Fprintln(os.Stderr, "  balancer   Control the balancer (start/stop/status)")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Use 'mammoth shard <subcommand> --help' for more information")
	}
	fs.Parse(args)

	if len(fs.Args()) == 0 {
		fs.Usage()
		os.Exit(1)
	}

	subcmd := fs.Args()[0]
	subArgs := fs.Args()[1:]

	switch subcmd {
	case "add":
		shardAddCmd(subArgs)
	case "remove":
		shardRemoveCmd(subArgs)
	case "list":
		shardListCmd(subArgs)
	case "status":
		shardStatusCmd(subArgs)
	case "enable":
		shardEnableCmd(subArgs)
	case "shard-key":
		shardKeyCmd(subArgs)
	case "split":
		shardSplitCmd(subArgs)
	case "move":
		shardMoveCmd(subArgs)
	case "balancer":
		shardBalancerCmd(subArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown shard subcommand: %s\n", subcmd)
		os.Exit(1)
	}
}

func shardAddCmd(args []string) {
	fs := flag.NewFlagSet("shard add", flag.ExitOnError)
	id := fs.String("id", "", "Shard ID (required)")
	host := fs.String("host", "", "Shard host:port (required)")
	maxSize := fs.Int64("max-size", 0, "Maximum size in GB (0=unlimited)")
	fs.Parse(args)

	if *id == "" || *host == "" {
		fmt.Fprintln(os.Stderr, "Error: -id and -host are required")
		fs.Usage()
		os.Exit(1)
	}

	// TODO: Connect to server and add shard
	fmt.Printf("Adding shard %s at %s...\n", *id, *host)
	if *maxSize > 0 {
		fmt.Printf("Max size: %d GB\n", *maxSize)
	}
	fmt.Println("Shard added successfully (not implemented)")
}

func shardRemoveCmd(args []string) {
	fs := flag.NewFlagSet("shard remove", flag.ExitOnError)
	id := fs.String("id", "", "Shard ID (required)")
	fs.Parse(args)

	if *id == "" {
		fmt.Fprintln(os.Stderr, "Error: -id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Removing shard %s...\n", *id)
	fmt.Println("Shard removed successfully (not implemented)")
}

func shardListCmd(args []string) {
	fs := flag.NewFlagSet("shard list", flag.ExitOnError)
	verbose := fs.Bool("v", false, "Verbose output")
	fs.Parse(args)

	// TODO: Connect to server and list shards
	fmt.Println("Shards:")
	fmt.Println("  shard1  localhost:27018  active  0 MB / unlimited")
	fmt.Println("  shard2  localhost:27019  active  0 MB / unlimited")
	fmt.Println("  shard3  localhost:27020  active  0 MB / unlimited")

	if *verbose {
		fmt.Println("\nChunks: 0 total")
		fmt.Println("Balancer: enabled, running")
	}
}

func shardStatusCmd(args []string) {
	fs := flag.NewFlagSet("shard status", flag.ExitOnError)
	fs.Parse(args)

	fmt.Println("Sharding Status:")
	fmt.Println("  Enabled: false")
	fmt.Println("  Config Server: not configured")
	fmt.Println("  Shards: 0")
	fmt.Println("  Databases: 0 sharded")
	fmt.Println("  Chunks: 0")
	fmt.Println("  Balancer: disabled")
}

func shardEnableCmd(args []string) {
	fs := flag.NewFlagSet("shard enable", flag.ExitOnError)
	db := fs.String("db", "", "Database name (required)")
	fs.Parse(args)

	if *db == "" {
		fmt.Fprintln(os.Stderr, "Error: -db is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Enabling sharding for database '%s'...\n", *db)
	fmt.Println("Sharding enabled (not implemented)")
}

func shardKeyCmd(args []string) {
	fs := flag.NewFlagSet("shard shard-key", flag.ExitOnError)
	ns := fs.String("ns", "", "Namespace (db.collection) (required)")
	fields := fs.String("fields", "", "Shard key fields, comma-separated (required)")
	hashed := fs.Bool("hashed", false, "Use hashed sharding")
	fs.Parse(args)

	if *ns == "" || *fields == "" {
		fmt.Fprintln(os.Stderr, "Error: -ns and -fields are required")
		fs.Usage()
		os.Exit(1)
	}

	fieldList := strings.Split(*fields, ",")
	fmt.Printf("Configuring shard key for %s...\n", *ns)
	fmt.Printf("Fields: %v\n", fieldList)
	if *hashed {
		fmt.Println("Type: hashed")
	} else {
		fmt.Println("Type: ranged")
	}
	fmt.Println("Shard key configured (not implemented)")
}

func shardSplitCmd(args []string) {
	fs := flag.NewFlagSet("shard split", flag.ExitOnError)
	chunkID := fs.String("chunk", "", "Chunk ID (required)")
	middle := fs.String("middle", "", "Split point (optional, auto if empty)")
	fs.Parse(args)

	if *chunkID == "" {
		fmt.Fprintln(os.Stderr, "Error: -chunk is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Splitting chunk %s...\n", *chunkID)
	if *middle != "" {
		fmt.Printf("At point: %s\n", *middle)
	} else {
		fmt.Println("Auto-detecting split point...")
	}
	fmt.Println("Chunk split (not implemented)")
}

func shardMoveCmd(args []string) {
	fs := flag.NewFlagSet("shard move", flag.ExitOnError)
	chunkID := fs.String("chunk", "", "Chunk ID (required)")
	toShard := fs.String("to", "", "Target shard ID (required)")
	fs.Parse(args)

	if *chunkID == "" || *toShard == "" {
		fmt.Fprintln(os.Stderr, "Error: -chunk and -to are required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Moving chunk %s to shard %s...\n", *chunkID, *toShard)
	fmt.Println("Chunk moved (not implemented)")
}

func shardBalancerCmd(args []string) {
	fs := flag.NewFlagSet("shard balancer", flag.ExitOnError)
	fs.Parse(args)

	if len(fs.Args()) == 0 {
		// Show status
		fmt.Println("Balancer Status:")
		fmt.Println("  State: stopped")
		fmt.Println("  Enabled: true")
		fmt.Println("  Rounds: 0")
		fmt.Println("  Chunks Moved: 0")
		return
	}

	action := fs.Args()[0]
	switch action {
	case "start":
		fmt.Println("Starting balancer...")
		fmt.Println("Balancer started (not implemented)")
	case "stop":
		fmt.Println("Stopping balancer...")
		fmt.Println("Balancer stopped (not implemented)")
	case "status":
		fmt.Println("Balancer Status:")
		fmt.Println("  State: stopped")
		fmt.Println("  Enabled: true")
	default:
		fmt.Fprintf(os.Stderr, "unknown balancer action: %s\n", action)
		os.Exit(1)
	}
}
