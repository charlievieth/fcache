// The fcache package provides a simple and fast SQLite based cache that is
// designed for CLIs and other tools that need to temporarily cache and
// retrieve data.
package fcache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mattn/go-sqlite3"
)

// DefaultBusyTimeout is the default busy timeout used when none is specfified.
//
// https://www.sqlite.org/pragma.html#pragma_busy_timeout
const DefaultBusyTimeout = 100 * time.Millisecond

// Schema table exists to future proof changes
const schemaVersion = 1

// NB: using an index on "expires_at_unix_ms" makes Prune much faster
// (10x with 2048 rows) but slows down insertion and update by ~30%
// for now the slow down in update/insert is worth the improved Prune
// performance (TBH: this is already way faster than it needs to be
// for the intended use case).
//
// NB: making "expires_at_unix_ms" NULLABLE doesn't help
const createCacheTablesStmt = `
CREATE TABLE IF NOT EXISTS cache (
	created_at_unix_ms INTEGER NOT NULL,
	expires_at_unix_ms INTEGER NOT NULL,
	key                TEXT PRIMARY KEY NOT NULL,
	data               BLOB NOT NULL,
	UNIQUE(key)        ON CONFLICT REPLACE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS
	cache_expires_at_unix_ms_idx
ON
	cache(expires_at_unix_ms)
WHERE
	expires_at_unix_ms >= 0;

CREATE TABLE IF NOT EXISTS schema (
	version         INTEGER NOT NULL,
	UNIQUE(version) ON CONFLICT IGNORE
);

INSERT INTO schema VALUES(1);`

// An Option configures a Cache.
type Option interface {
	apply(*Cache)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*Cache)

func (f optionFunc) apply(cache *Cache) {
	f(cache)
}

// WithBusyTimeout sets the busy timeout of the Cache.
//
// In addition to SQLite3's busy timeout logic the Cache will also retry
// queries that fail with SQLITE_BUSY or SQLITE_LOCKED until the busy timeout
// expires. This is to handle the busy timeout setting of SQlite3 not always
// working as expected.
//
// https://www.sqlite.org/pragma.html#pragma_busy_timeout
func WithBusyTimeout(d time.Duration) Option {
	if d < 0 {
		d = 0
	}
	return optionFunc(func(c *Cache) {
		c.busyTimeout = d
	})
}

// DisallowUnknownFields causes the Cache's Decoder to return an error when the
// destination is a struct and the input contains object keys which do not match
// any non-ignored, exported fields in the destination.
func DisallowUnknownFields() Option {
	return optionFunc(func(c *Cache) {
		c.strictJSON = true
	})
}

// A Cache is a fast SQLite based on-disk cache.
type Cache struct {
	once        sync.Once
	writeDB     *sql.DB // Write conn, only one exists
	readDB      *sql.DB // Read conn, multiple exist
	err         error   // init error, if any
	busyTimeout time.Duration
	filename    string
	strictJSON  bool // Disallow unknown fields when unmarshaling JSON
}

// Open returns a new Cache with filename as the SQLite3 database path.
// If filename does not exist, it will be created (parent directories
// are not created). It is allowed, but not always recommended, to use
// ":memory:" as the filename, which will create an in-memory cache.
//
// Only one [Cache] should be opened per-process. If multiple connections to
// the database are opened (either by the same process or by multiple processes)
// there is a chance that [sqlite3.ErrLocked] errors will occur. This library
// will retry ops that return [sqlite3.ErrLocked] up to the configured busy
// timeout.
func Open(filename string, opts ...Option) (*Cache, error) {
	cache := &Cache{
		busyTimeout: DefaultBusyTimeout,
		filename:    filename,
	}
	for _, o := range opts {
		o.apply(cache)
	}
	dsn := url.Values{
		"_busy_timeout": []string{strconv.FormatInt(cache.busyTimeout.Milliseconds(), 10)},
		"_cache_size":   []string{"-20000"},
		"_journal_mode": []string{"WAL"},
		"_synchronous":  []string{"NORMAL"},
		"_txlock:":      []string{"immediate"},
	}
	writeDB, err := sql.Open("sqlite3", "file:"+filename+"?"+dsn.Encode())
	if err != nil {
		return nil, fmt.Errorf("fcache: error opening database for writing: %w", err)
	}
	writeDB.SetMaxOpenConns(1) // Only 1 writer
	cache.writeDB = writeDB

	dsn.Set("_query_only", "true")
	readDB, err := sql.Open("sqlite3", "file:"+filename+"?"+dsn.Encode())
	if err != nil {
		return nil, fmt.Errorf("fcache: error opening database for reading: %w", err)
	}
	readDB.SetMaxOpenConns(max(4, runtime.NumCPU()))
	cache.readDB = readDB

	return cache, nil
}

func userCacheDir() (string, error) {
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return dir, nil
	}
	return os.UserCacheDir()
}

func validateCacheName(name string) error {
	if name == "" {
		return errors.New("fcache: cache name must not be empty")
	}
	if name == "." {
		return errors.New(`fcache: invalid cache name: "."`)
	}
	if !utf8.ValidString(name) {
		return errors.New(`fcache: cache name contains invalid UTF-8`)
	}
	switch trimmed := strings.TrimSpace(name); {
	case trimmed == "":
		return errors.New("fcache: cache name must not contain only spaces")
	case trimmed != name:
		return errors.New("fcache: cache name may not start or end with whitespace")
	}
	// Check for names that could be used to write outside of the
	// cache directory.
	if strings.Contains(name, "..") {
		return errors.New(`fcache: cache name may not contain: ".."`)
	}
	if filepath.Clean(name) != name {
		return errors.New("fcache: filepath.Clean modifies the cache name")
	}
	return nil
}

// OpenUserCache returns a new cache using the default cache directory for the
// current OS. The $XDG_CACHE_HOME environment variable is respected on all
// systems (not just Linux) otherwise [os.UserCacheDir] is used.
//
// On Linux cache directory might be "$HOME/.cache/fcache/{NAME}/cache.sqlite3"
// and on macOS it might be "$HOME/Library/Caches/fcache/{NAME}/cache.sqlite3".
//
// Name is the name of the directory to create under the user cache directory
// and the database name is always "cache.sqlite3".
//
// The name may contain multiple slashes, which will create a sub-directories.
// This is useful for grouping caches by project (e.g. "project/cache_1",
// "project/cache_2"). The name may not contain ".." or any other sequence
// that would cause it to be created outside of the user cache directory.
func OpenUserCache(name string, opts ...Option) (*Cache, error) {
	if err := validateCacheName(name); err != nil {
		return nil, err
	}
	cache, err := userCacheDir()
	if err != nil {
		return nil, fmt.Errorf("fcache: failed to locate user cache directory: %w", err)
	}
	dir := filepath.Join(cache, "fcache", name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("fcache: failed to create cache directory: %w", err)
	}
	return Open(filepath.Join(dir, "cache.sqlite3"), opts...)
}

// Database returns the file path of the Cache's database.
func (c *Cache) Database() string { return c.filename }

// Close closes the Cache and the underlying database.
func (c *Cache) Close() error {
	closeDB := func(db *sql.DB) error {
		if db != nil {
			return db.Close()
		}
		return errors.New("not initialized")
	}
	we := closeDB(c.writeDB)
	re := closeDB(c.readDB)
	switch {
	case we != nil && re == nil:
		return fmt.Errorf("fcache: closing write database: %w", we)
	case we == nil && re != nil:
		return fmt.Errorf("fcache: closing read database: %w", re)
	case we != nil && re != nil:
		return fmt.Errorf("fcache: closing databases: write db: %w read db: %w", we, re)
	}
	return nil
}

// isRetryableErr returns true if the error is transient and the query should
// be retried.
func isRetryableErr(err error) bool {
	if err == nil {
		return false
	}
	// Limit ourselves to an 50 nested errors - this is extremely
	// unlikely, but it's best to avoid unbounded loops.
	for i := 0; err != nil && i < 50; i++ {
		switch e := err.(type) {
		case sqlite3.Error:
			// The sqlite3 library returns the error as a value.
			return e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked
		case *sqlite3.Error:
			// Handle the error being wrapped as a pointer.
			return e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked
		}
		err = errors.Unwrap(err)
	}
	return false
}

func stopTicker(t *time.Ticker) {
	if t != nil {
		t.Stop()
	}
}

func (c *Cache) retryNoInit(ctx context.Context, fn func() error) (err error) {
	if c.busyTimeout <= 0 {
		return fn()
	}
	var tick *time.Ticker
	to := getTimer(c.busyTimeout)
	defer putTimer(to)
	defer stopTicker(tick)
	done := ctx.Done()
Loop:
	for {
		if err = fn(); err == nil || !isRetryableErr(err) {
			break
		}
		// Retry the DB op (often due to the database being locked), but
		// respect the configured busy timeout.
		if tick == nil {
			// Lazily initialize the ticker.
			tick = time.NewTicker(5 * time.Millisecond)
		}
		select {
		case <-tick.C:
			// Ok
		case <-done:
			err = ctx.Err()
			break Loop
		case <-to.C:
			break Loop // timed out
		}
	}
	return err
}

func (c *Cache) retryWrite(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	if err := c.lazyInit(ctx); err != nil {
		return err
	}
	return c.retryNoInit(ctx, func() error {
		tx, err := c.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

func (c *Cache) retryRead(ctx context.Context, fn func(db *sql.DB) error) (err error) {
	if err := c.lazyInit(ctx); err != nil {
		return err
	}
	return c.retryNoInit(ctx, func() error {
		return fn(c.readDB)
	})
}

// NB: this is always read-only
func (c *Cache) retryRows(ctx context.Context, query string) (*sql.Rows, error) {
	var rows *sql.Rows
	err := c.retryRead(ctx, func(db *sql.DB) (err error) {
		rows, err = db.QueryContext(ctx, query)
		return err
	})
	if err != nil {
		return nil, err
	}
	if rows == nil {
		// This should never happen.
		return nil, errors.New("fcache: internal error: nil rows")
	}
	return rows, nil
}

func (c *Cache) lazyInit(ctx context.Context) error {
	const tablesExistQuery = `
	SELECT
		COUNT(*)
	FROM
		sqlite_master
	WHERE
		type = 'table' AND (name = 'cache' OR name = 'schema');`
	c.once.Do(func() {
		// Fast check for the tables existing. Ignore the error since it
		// is likely due to the tables not existing.
		var count int64
		err := c.retryNoInit(ctx, func() error {
			return c.writeDB.QueryRowContext(ctx, tablesExistQuery).Scan(&count)
		})
		if err != nil {
			// This should only happen if the context is cancelled.
			c.err = fmt.Errorf("fcache: error checking if cache tables exist: %w", err)
			return
		}
		if count == 2 {
			return
		}
		err = c.retryNoInit(ctx, func() error {
			// NB(charlie): this is fast enough and any attempt to make it
			// faster has not yielded a significant improvement - so stop.
			tx, err := c.writeDB.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, createCacheTablesStmt)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		})
		// Wrap the error outside of the retry handler to make detection of
		// busy errors simpler/faster.
		if err != nil {
			c.err = fmt.Errorf("fcache: failed to initialize database (%s): %w",
				c.filename, err)
		}
	})
	return c.err
}

// RawMessage is an alias of [json.RawMessage] and can be used to pass an
// already JSON encoded raw value to the cache without it having to be
// marshaled again.
//
// RawMessage is a raw encoded JSON value.
// It implements [Marshaler] and [Unmarshaler] and can
// be used to delay JSON decoding or precompute a JSON encoding.
type RawMessage json.RawMessage

// StoreContext encodes val to JSON and sets it as the value for key, replacing
// any existing value. If the value cannot be JSON encoded the returned error
// will unwrap to: json.UnsupportedTypeError.
//
// The TTL controls when an entry expires and has millisecond resolution.
// A negative TTL never expires.
//
// [RawMessage] can be used to pass a raw encoded JSON value and skip marshaling.
func (c *Cache) StoreContext(ctx context.Context, key string, val any, ttl time.Duration) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("fcache: cannot JSON encode (%T): %w", val, err)
	}
	createdAt := time.Now().UnixMilli()
	expiresAt := int64(-1)
	if ttl >= 0 {
		expiresAt = createdAt + ttl.Milliseconds()
	}
	const query = `
	INSERT INTO cache (
		created_at_unix_ms,
		expires_at_unix_ms,
		key,
		data
	) VALUES (?, ?, ?, ?);`
	err = c.retryWrite(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, query, createdAt, expiresAt, key, data)
		return err
	})
	if err != nil {
		return fmt.Errorf("fcache: failed to store value: %w", err)
	}
	return nil
}

// Store encodes val to JSON and sets it as the value for key, replacing any
// existing value. If the value cannot be JSON encoded the returned error will
// unwrap to: json.UnsupportedTypeError.
//
// The TTL controls when an entry expires and has millisecond resolution.
// A negative TTL never expires.
//
// [RawMessage] can be used to pass a raw encoded JSON value and skip marshaling.
func (c *Cache) Store(key string, v any, ttl time.Duration) error {
	return c.StoreContext(context.Background(), key, v, ttl)
}

func isNoRowsErr(err error) bool {
	return err == sql.ErrNoRows || errors.Is(err, sql.ErrNoRows)
}

func (c *Cache) unmarshal(data []byte, dst any) error {
	if c.strictJSON {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		return dec.Decode(dst)
	}
	return json.Unmarshal(data, dst)
}

// LoadContext stores the value for key in dst, which must not be nil, and
// returns true if an entry for key exists and is not expired. If the entry
// is expired it will still be stored in dst and false will be returned.
//
// An error is returned if there was an error querying the cache database or
// if there was an error unmarshaling the JSON value into dst.
//
// If the DisallowUnknownFields option is enabled an error is returned when the
// destination is a struct and the input contains object keys which do not match
// any non-ignored, exported fields in the destination.
func (c *Cache) LoadContext(ctx context.Context, key string, dst any) (bool, error) {
	const query = `
	SELECT
		expires_at_unix_ms, data
	FROM
		cache
	WHERE
		key = ? LIMIT 1;`
	var data []byte
	var expiredAt int64
	err := c.retryRead(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, query, key).Scan(&expiredAt, &data)
	})
	if err != nil {
		if isNoRowsErr(err) {
			return false, nil
		}
		return false, fmt.Errorf("fcache: error loading value: %w", err)
	}
	if err := c.unmarshal(data, dst); err != nil {
		return false, fmt.Errorf("fcache: cannot store value in (%T): %w", dst, err)
	}
	return expiredAt == -1 || time.Now().UnixMilli() < expiredAt, nil
}

// Load stores the value for key in dst, which must not be nil, and
// returns true if an entry for key exists and is not expired. If the entry
// is expired it will still be stored in dst and false will be returned.
//
// An error is returned if there was an error querying the cache database or
// if there was an error unmarshaling the JSON value into dst.
//
// If the DisallowUnknownFields option is enabled an error is returned when the
// destination is a struct and the input contains object keys which do not match
// any non-ignored, exported fields in the destination.
func (c *Cache) Load(key string, dst any) (bool, error) {
	return c.LoadContext(context.Background(), key, dst)
}

// KeysContext returns a sorted slice of all the keys in the cache.
func (c *Cache) KeysContext(ctx context.Context) ([]string, error) {
	rows, err := c.retryRows(ctx, `SELECT key FROM cache ORDER BY key;`)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			break
		}
		keys = append(keys, key)
	}
	if err != nil {
		rows.Close()
		return keys, err
	}
	if err := rows.Err(); err != nil {
		return keys, err
	}
	return keys, nil
}

// Keys returns a sorted slice of all the keys in the cache.
func (c *Cache) Keys() ([]string, error) {
	return c.KeysContext(context.Background())
}

type fcacheError struct {
	msg string
	err error
}

func wrapError(cause string, err error) error {
	if err == nil {
		return nil
	}
	return &fcacheError{msg: cause, err: err}
}

func (f *fcacheError) Error() string {
	if strings.HasPrefix(f.msg, "fcache: ") {
		return f.msg + ": " + f.err.Error()
	}
	return "fcache: " + f.msg + ": " + f.err.Error()
}

func (c *Cache) wrapError(cause string, err error) error {
	if err == nil {
		return nil
	}
	if err == c.err {
		return c.err
	}
	return wrapError(cause, err)
}

// DeleteContext deletes the entry with key from the cache and returns true if
// an entry with key was deleted. If no entry with key exists false is returned.
func (c *Cache) DeleteContext(ctx context.Context, key string) (bool, error) {
	const query = `DELETE FROM cache WHERE key = ?;`
	var found bool
	err := c.retryWrite(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, query, key)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		found = n > 0
		return nil
	})
	if err != nil {
		err = fmt.Errorf("fcache: delete: %w", err)
	}
	return found, err
}

// Delete deletes the entry with key from the cache and returns true if an entry
// with key was deleted. If no entry with key exists false is returned.
func (c *Cache) Delete(key string) (bool, error) {
	return c.DeleteContext(context.Background(), key)
}

func bindParams(n int) string {
	switch n {
	case 0:
		return ""
	case 1:
		return "?"
	case 2:
		return "?, ?"
	case 3:
		return "?, ?, ?"
	case 4:
		return "?, ?, ?, ?"
	case 5:
		return "?, ?, ?, ?, ?"
	case 6:
		return "?, ?, ?, ?, ?, ?"
	default:
		return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
	}
}

// DeleteManyContext deletes the entries with keys from the cache and returns
// true if at least one entry was deleted. If no entries were deleted false is
// returned.
func (c *Cache) DeleteManyContext(ctx context.Context, keys ...string) (bool, error) {
	if len(keys) == 0 {
		return false, nil
	}
	query := `DELETE FROM cache WHERE key IN (` + bindParams(len(keys)) + ");"
	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}
	var found bool
	err := c.retryWrite(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		found = n > 0
		return nil
	})
	return found, err
}

// DeleteMany deletes the entries with keys from the cache and returns true if
// at least one entry was deleted. If no entries were deleted false is returned.
func (c *Cache) DeleteMany(keys ...string) (bool, error) {
	return c.DeleteManyContext(context.Background(), keys...)
}

// CountContext returns the number of entries in the cache.
func (c *Cache) CountContext(ctx context.Context) (int64, error) {
	const query = `SELECT COUNT(*) FROM cache;`
	var n int64
	err := c.retryRead(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, query).Scan(&n)
	})
	return n, err
}

// CountContext returns the number of entries in the cache.
func (c *Cache) Count() (int64, error) {
	return c.CountContext(context.Background())
}

// ExpiredCountContext return the number of expired entries in the cache.
func (c *Cache) ExpiredCountContext(ctx context.Context) (int64, error) {
	const query = `
	SELECT COUNT(*) FROM
		cache
	WHERE
		0 <= expires_at_unix_ms AND expires_at_unix_ms < ?;`
	var n int64
	ts := time.Now().UnixMilli()
	err := c.retryRead(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, query, ts).Scan(&n)
	})
	return n, err
}

// ExpiredCount return the number of expired entries in the cache.
func (c *Cache) ExpiredCount() (int64, error) {
	return c.ExpiredCountContext(context.Background())
}

// ContainsContext returns true if an entry exists for key. It does not check if
// the entry is expired.
func (c *Cache) ContainsContext(ctx context.Context, key string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM cache WHERE key = ?);`
	var exists bool
	err := c.retryRead(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, query, key).Scan(&exists)
	})
	return exists, err
}

// Contains returns true if an entry exists for key. It does not check if
// the entry is expired.
func (c *Cache) Contains(key string) (bool, error) {
	return c.ContainsContext(context.Background(), key)
}

// EntryContext returns the Entry for key or ErrNoRows if it does not exist.
func (c *Cache) EntryContext(ctx context.Context, key string) (*Entry, error) {
	const query = `SELECT * FROM cache WHERE key = ? LIMIT 1;`
	var (
		e         Entry
		createdAt int64
		expiresAt int64
	)
	err := c.retryRead(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, query, key).Scan(&createdAt, &expiresAt, &e.Key, &e.Data)
	})
	if err != nil {
		return nil, err
	}
	e.CreatedAt = time.UnixMilli(createdAt)
	if expiresAt >= 0 {
		e.ExpiresAt = time.UnixMilli(expiresAt)
	}
	return &e, nil
}

// Entry returns the Entry for key or ErrNoRows if it does not exist.
func (c *Cache) Entry(key string) (*Entry, error) {
	return c.EntryContext(context.Background(), key)
}

// EntriesContext returns a slice of all the entries in the cache sorted by key.
// [sql.ErrNoRows] is returned if there are no entries.
func (c *Cache) EntriesContext(ctx context.Context) ([]Entry, error) {
	rows, err := c.retryRows(ctx, `SELECT * FROM cache ORDER BY key;`)
	if err != nil {
		return nil, err
	}
	var ents []Entry
	for i := 0; rows.Next(); i++ {
		var (
			e         Entry
			createdAt int64
			expiresAt int64
		)
		if err = rows.Scan(&createdAt, &expiresAt, &e.Key, &e.Data); err != nil {
			break
		}
		e.CreatedAt = time.UnixMilli(createdAt)
		if expiresAt >= 0 {
			e.ExpiresAt = time.UnixMilli(expiresAt)
		}
		ents = append(ents, e)
	}
	if err != nil {
		_ = rows.Close()
		return ents, err
	}
	return ents, rows.Err()
}

// Entries returns a slice of all the entries in the cache sorted by key.
// [sql.ErrNoRows] is returned if there are no entries.
func (c *Cache) Entries() ([]Entry, error) {
	return c.EntriesContext(context.Background())
}

// PruneContext prunes any expired entries from the cache and performs a VACUUM
// if any entries were removed.
func (c *Cache) PruneContext(ctx context.Context) error {
	const query = `
	DELETE FROM
		cache
	WHERE
		0 <= expires_at_unix_ms AND expires_at_unix_ms < ?;`
	var rowsDeleted int64
	err := c.retryWrite(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, query, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		rowsDeleted, _ = res.RowsAffected()
		return nil
	})
	if err != nil {
		return fmt.Errorf("fcache: prune: %w", err)
	}
	if rowsDeleted > 0 {
		// Can't run VACUUM in a transaction
		err := c.retryNoInit(ctx, func() error {
			_, err := c.writeDB.ExecContext(ctx, "VACUUM;")
			return err
		})
		if err != nil {
			return fmt.Errorf("fcache: vacuum: %w", err)
		}
	}
	return nil
}

// Prune prunes any expired entries from the cache and performs a VACUUM
// if any entries were removed.
func (c *Cache) Prune() error {
	return c.PruneContext(context.Background())
}

// An Entry is a cache entry.
type Entry struct {
	Key       string    `json:"key" db:"key"`
	CreatedAt time.Time `json:"created_at" db:"created_at_unix_ms"`
	// ExpiresAt is zero if the entry was created without a TTL.
	ExpiresAt time.Time       `json:"expires_at" db:"expires_at_unix_ms"`
	Data      json.RawMessage `json:"data" db:"data"`
}

// HasTTL returns true if the Entry was created with a TTL.
func (e *Entry) HasTTL() bool {
	return !e.ExpiresAt.IsZero()
}

// TTL returns the TTL the Entry was created with or -1 if the Entry does not
// have a TTL.
func (e *Entry) TTL() time.Duration {
	if e.HasTTL() {
		return e.ExpiresAt.Sub(e.CreatedAt)
	}
	return -1
}

// Unmarshal is a helper to unmarshal the data stored in the Entry into v.
func (e *Entry) Unmarshal(v any) error {
	return json.Unmarshal(e.Data, &v)
}

// Expired returns if the Entry is expired.
func (e *Entry) Expired() bool {
	return e.HasTTL() && e.ExpiresAt.Before(time.Now())
}

var timerPool sync.Pool

func getTimer(d time.Duration) *time.Timer {
	if t, _ := timerPool.Get().(*time.Timer); t != nil {
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

func putTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}
