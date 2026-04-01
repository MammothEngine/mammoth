package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func userCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: mammoth user <create|list|delete> [options]")
		os.Exit(1)
	}

	switch args[0] {
	case "create":
		userCreateCmd(args[1:])
	case "list":
		userListCmd(args[1:])
	case "delete":
		userDeleteCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown user subcommand: %s\n", args[0])
		os.Exit(1)
	}
}

func userCreateCmd(args []string) {
	fs := flag.NewFlagSet("user create", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	dbName := fs.String("db", "admin", "authentication database")
	username := fs.String("username", "", "username")
	password := fs.String("password", "", "password")
	role := fs.String("role", "readWrite", "role (read, readWrite, dbAdmin, userAdmin, root)")
	fs.Parse(args)

	if *username == "" || *password == "" {
		fmt.Fprintln(os.Stderr, "Error: --username and --password are required")
		os.Exit(1)
	}

	eng, err := openEngine(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	userStore := auth.NewUserStore(eng)
	if err := userStore.CreateUser(*username, *dbName, *password); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating user: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("User '%s' created in database '%s' with role '%s'\n", *username, *dbName, *role)
}

func userListCmd(args []string) {
	fs := flag.NewFlagSet("user list", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	dbName := fs.String("db", "", "filter by database (empty = all)")
	fs.Parse(args)

	eng, err := openEngine(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	userStore := auth.NewUserStore(eng)

	if *dbName != "" {
		users, err := userStore.GetUsersInDB(*dbName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Users in '%s':\n", *dbName)
		for _, u := range users {
			fmt.Printf("  %-20s  db: %-10s  created: %d\n", u.Username, u.AuthDB, u.CreatedAt)
		}
		if len(users) == 0 {
			fmt.Println("  (none)")
		}
		return
	}

	// List all users across all databases
	fmt.Println("All users:")
	cat := mongo.NewCatalog(eng)
	dbs, err := cat.ListDatabases()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	found := false
	for _, db := range dbs {
		users, err := userStore.GetUsersInDB(db.Name)
		if err != nil || len(users) == 0 {
			continue
		}
		for _, u := range users {
			fmt.Printf("  %-20s  db: %-10s  created: %d\n", u.Username, u.AuthDB, u.CreatedAt)
			found = true
		}
	}

	if !found {
		fmt.Println("  (none)")
	}
}

func userDeleteCmd(args []string) {
	fs := flag.NewFlagSet("user delete", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	dbName := fs.String("db", "admin", "authentication database")
	username := fs.String("username", "", "username to delete")
	fs.Parse(args)

	if *username == "" {
		fmt.Fprintln(os.Stderr, "Error: --username is required")
		os.Exit(1)
	}

	eng, err := openEngine(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	userStore := auth.NewUserStore(eng)
	if err := userStore.DropUser(*username, *dbName); err != nil {
		fmt.Fprintf(os.Stderr, "Error deleting user: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("User '%s' deleted from database '%s'\n", *username, *dbName)
}

func openEngine(dataDir string) (*engine.Engine, error) {
	opts := engine.DefaultOptions(dataDir)
	return engine.Open(opts)
}
