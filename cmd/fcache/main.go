package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/charlievieth/fcache"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/term"
)

var root = cobra.Command{
	Use: "fcache",
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func digits(b []byte) bool {
	if len(b) > 20 {
		return false
	}
	if len(b) > 0 && b[0] == '-' {
		b = b[1:]
	}
	for _, c := range b {
		if !isDigit(c) {
			return false
		}
	}
	return true
}

func digitsDot(b []byte) bool {
	if len(b) > 1024 {
		return false // reject extremely large floating point values
	}
	dot := false
	for _, c := range b {
		if !isDigit(c) {
			if c == '.' && !dot {
				dot = true
				continue
			}
			return false
		}
	}
	if dot && len(b) == 1 {
		return false
	}
	return true
}

func readValueArgument(arg string) (any, error) {
	switch arg {
	case "":
		return nil, errors.New("empty value argument")
	case "nil":
		return nil, errors.New("empty value argument")
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "-":
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(os.Stdin); err != nil {
			return nil, err
		}
		arg = buf.String()
	}
	// Try to parse the arg as an int/float so that we set the correct type
	if len(arg) <= 21 {
		if i, err := strconv.ParseInt(arg, 10, 64); err == nil {
			return i, nil
		}
		if u, err := strconv.ParseUint(arg, 10, 64); err == nil {
			return u, nil
		}
	}
	if f, err := strconv.ParseFloat(arg, 64); err == nil {
		return f, nil
	}
	if b := []byte(arg); json.Valid(b) {
		// NB: no need to compact the JSON - the encoder does that for us
		return json.RawMessage(b), nil
	}
	return arg, nil
}

func mustB(s *pflag.FlagSet, name string) bool {
	ok, err := s.GetBool(name)
	if err != nil {
		panic(err)
	}
	return ok
}

func realMain() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	root := cobra.Command{
		Use: "fcache",
	}
	pflags := root.PersistentFlags()
	cacheFile := pflags.StringP("cache-file", "f", "", "cache file")
	userCache := pflags.StringP("user-cache", "u", "", "name of user cache")
	root.MarkFlagsMutuallyExclusive("cache-file", "user-cache")
	root.MarkPersistentFlagFilename("cache-file")

	openCache := func(opts ...fcache.Option) (*fcache.Cache, error) {
		if *cacheFile != "" {
			return fcache.New(*cacheFile, opts...)
		}
		if *userCache != "" {
			return fcache.NewUserCache(*userCache, opts...)
		}
		return nil, errors.New("cache must be specified")
	}

	storeCmd := &cobra.Command{
		Use: "store KEY VALUE",
		// TODO: return all keys if
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("expected 2 args got: %d", len(args))
			}
			ttl, err := cmd.Flags().GetDuration("ttl")
			if err != nil {
				return err
			}
			db, err := openCache()
			if err != nil {
				return err
			}
			defer db.Close()

			key := args[0]
			val, err := readValueArgument(args[1])
			if err != nil {
				return err
			}

			return db.StoreContext(cmd.Context(), key, val, ttl)
		},
	}
	storeCmd.Flags().Duration("ttl", -1, "entry TTL")

	// TODO: exit 1 if no match was found and >1 if there was an error
	loadCmd := &cobra.Command{
		Use: "load",
		// TODO: return all keys if
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openCache(fcache.ReadOnly())
			if err != nil {
				return err
			}
			defer db.Close()

			ck := func(name string) bool {
				return mustB(cmd.Flags(), name)
			}

			var dst json.RawMessage
			if _, err := db.LoadContext(cmd.Context(), args[0], &dst); err != nil {
				return err
			}
			if !ck("raw") {
				if ck("pretty") {
					var buf bytes.Buffer
					if err := json.Indent(&buf, dst, "", "    "); err != nil {
						return err
					}
					dst = buf.Bytes()
				}
				if _, err := os.Stdout.Write(dst); err != nil {
					return err
				}
				return nil
			}
			var val any
			if err := json.Unmarshal(dst, &val); err != nil {
				return err
			}
			// TODO: this behavior should be optional
			switch v := val.(type) {
			case []any:
				for _, s := range v {
					if _, err := fmt.Println(s); err != nil {
						return err
					}
				}
			default:
				if _, err := fmt.Println(val); err != nil {
					return err
				}
			}
			return nil
		},
	}
	loadCmd.Flags().Bool("raw", false, "print raw JSON values")
	loadCmd.Flags().Bool("pretty", false, "pretty print JSON value")

	dumpCmd := &cobra.Command{
		Use:  "dump",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := openCache(fcache.ReadOnly())
			if err != nil {
				return err
			}
			defer c.Close()

			ents, err := c.EntriesContext(ctx)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			if term.IsTerminal(syscall.Stdout) {
				enc.SetIndent("", "    ")
			}
			return enc.Encode(ents)
		},
	}

	pruneCmd := &cobra.Command{
		Use:  "prune",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := openCache()
			if err != nil {
				return err
			}
			defer c.Close()

			if err := c.Prune(); err != nil {
				return err
			}
			return nil
		},
	}

	root.AddCommand(storeCmd, loadCmd, dumpCmd, pruneCmd)
	root.SetContext(ctx)
	return root.Execute()
}

// type entry fcache.Entry

// type entry struct {
// 	Key       string     `json:"key"`
// 	CreatedAt time.Time  `json:"created_at"`
// 	ExpiresAt *time.Time `json:"expires_at,omitempty"`
// 	Data      string     `json:"data"`
// }

// func (e *entry) MarshalJSON() ([]byte, error) {
// 	ent := struct {
// 		Key       string          `json:"key"`
// 		CreatedAt time.Time       `json:"created_at"`
// 		ExpiresAt *time.Time      `json:"expires_at,omitempty"`
// 		Data      json.RawMessage `json:"data"`
// 	}{
// 		Key:       e.Key,
// 		CreatedAt: e.CreatedAt,
// 		Data:      e.Data,
// 	}
// 	x := fcache.Entry(*e)
// 	_ = x
// 	if e.HasTTL() {
// 		ent.ExpiresAt = &e.ExpiresAt
// 	}
// 	return json.Marshal(ent)
// }

func main() {
	if err := realMain(); err != nil {
		os.Exit(1)
	}
}
