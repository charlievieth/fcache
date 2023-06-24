package fcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// A Cache is a fast SQLite based on-disk cache.
type Cache struct {
	once sync.Once
	db   *sql.DB
	err  error // init error, if any
}

////////////////////////////////////////////////////////////////////////////////
//
// TODO:
// 	1. store the TTL with the value
// 	2. record times since last VACUUM
//
////////////////////////////////////////////////////////////////////////////////

// TODO: using an index on "expires_at_unix_ms" makes this much faster
// (10x with 2048 rows) when there are many rows but slows down insertion
// and update by ~30%.
//
// Place fixed size timestamps first to reduce size
const createCacheTableStmt = `
CREATE TABLE IF NOT EXISTS cache (
	created_at_unix_ms INTEGER NOT NULL,
	expires_at_unix_ms INTEGER NOT NULL,
	key                TEXT PRIMARY KEY NOT NULL,
	data               TEXT NOT NULL,
	UNIQUE(key)        ON CONFLICT REPLACE
) WITHOUT ROWID;`

// New returns a new fcache with database path
func New(database string) (*Cache, error) {
	db, err := sql.Open("sqlite3", "file:"+database+
		"?_busy_timeout=200&_journal_mode=WAL") // 200ms busy timeout
	if err != nil {
		return nil, err
	}
	return &Cache{db: db}, nil
}

func userCacheDir() (string, error) {
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return dir, nil
	}
	return os.UserCacheDir()
}

// NewUserCache returns a new cache using the default cache directory for the
// current OS. The $XDG_CACHE_HOME environment variable is respected on all
// systems (not just Linux).
//
// On Linux this might be "$HOME/.cache/{NAME}/cache.sqlite3"
func NewUserCache(name string) (*Cache, error) {
	cache, err := userCacheDir()
	if err != nil {
		return nil, err
	}
	dir := cache + string(filepath.Separator) + name
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return New(filepath.Join(dir, "cache.sqlite3"))
}

// Close closes the Cache and the underlying database.
func (c *Cache) Close() error { return c.db.Close() }

func (c *Cache) lazyInit(ctx context.Context) error {
	c.once.Do(func() {
		_, c.err = c.db.ExecContext(ctx, createCacheTableStmt)
	})
	return c.err
}

// StoreContext encodes val to JSON and sets it as the value for key, replacing
// any existing value. If the value cannot be JSON encoded a
// json.UnsupportedTypeError error is returned.
//
// The TTL controls when an entry expires and has millisecond resolution.
// A negative TTL never expires.
func (c *Cache) StoreContext(ctx context.Context, key string, val any, ttl time.Duration) error {
	now := time.Now()
	exp := int64(-1)
	if ttl >= 0 {
		exp = now.Add(ttl).UnixMilli()
	}
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := c.lazyInit(ctx); err != nil {
		return err
	}
	const query = `
	INSERT INTO cache (
		created_at_unix_ms,
		expires_at_unix_ms,
		key,
		data
	) VALUES (?, ?, ?, ?);`
	if _, err := c.db.ExecContext(ctx, query, now.UnixMilli(), exp, key, data); err != nil {
		return err
	}
	return nil
}

// StoreContext encodes val to JSON and sets it as the value for key, replacing
// any existing value. If the value cannot be JSON encoded a
// json.UnsupportedTypeError error is returned.
//
// The TTL controls when an entry expires and has millisecond resolution.
// A negative TTL never expires.
func (c *Cache) Store(key string, v any, ttl time.Duration) error {
	return c.StoreContext(context.Background(), key, v, ttl)
}

func isNoRows(err error) bool {
	return err == sql.ErrNoRows || errors.Is(err, sql.ErrNoRows)
}

// LoadContext loads the value stored in the cache for key and, if found and
// the entry has not expired, stores it in dst. LoadContext returns true if
// the value associated with key was found and not expired.
func (c *Cache) LoadContext(ctx context.Context, key string, dst any) (bool, error) {
	if err := c.lazyInit(ctx); err != nil {
		return false, err
	}
	const query = `
	SELECT
		expires_at_unix_ms, data
	FROM
		cache
	WHERE
		key = ? LIMIT 1;`
	var data []byte
	var expiredAt int64
	if err := c.db.QueryRowContext(ctx, query, key).Scan(&expiredAt, &data); err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, err
	}
	if expiredAt != -1 && expiredAt < time.Now().UnixMilli() {
		return false, nil
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return false, err
	}
	return true, nil
}

// Load loads the value stored in the cache for key and, if found and the entry
// has not expired, stores it in dst. LoadContext returns true if the value
// associated with key was found and not expired.
func (c *Cache) Load(key string, dst any) (bool, error) {
	return c.LoadContext(context.Background(), key, dst)
}

// EntriesContext returns a slice of all the entries in the cache.
func (c *Cache) EntriesContext(ctx context.Context) ([]Entry, error) {
	if err := c.lazyInit(ctx); err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, `SELECT * FROM cache ORDER BY key;`)
	if err != nil {
		return nil, err
	}
	var ents []Entry
	for rows.Next() {
		var e Entry
		var createdAt int64
		var expiresAt int64
		if err := rows.Scan(&createdAt, &expiresAt, &e.Key, &e.Data); err != nil {
			return ents, err
		}
		e.CreatedAt = time.UnixMilli(createdAt)
		if expiresAt >= 0 {
			e.ExpiresAt = time.UnixMilli(expiresAt)
		}
		ents = append(ents, e)
	}
	if err := rows.Err(); err != nil {
		return ents, err
	}
	return ents, nil
}

// Entries returns a slice of all the entries in the cache sorted by key.
func (c *Cache) Entries() ([]Entry, error) {
	return c.EntriesContext(context.Background())
}

// PruneContext prunes any expired entries from the cache.
func (c *Cache) PruneContext(ctx context.Context) error {
	const query = `
	DELETE FROM
		cache
	WHERE
		0 <= expires_at_unix_ms AND expires_at_unix_ms < ?;`
	res, err := c.db.ExecContext(ctx, query, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		if _, err := c.db.ExecContext(ctx, "VACUUM"); err != nil {
			return err
		}
	}
	return nil
}

// PruneContext prunes any expired entries from the cache.
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

// Expired returns if the Entry is expired.
func (e *Entry) Expired() bool {
	return !e.ExpiresAt.IsZero() && e.ExpiresAt.Before(time.Now())
}

// TTL returns the TTL the Entry was created with.
func (e *Entry) TTL() time.Duration {
	if e.ExpiresAt.IsZero() {
		return -1
	}
	return e.ExpiresAt.Sub(e.CreatedAt)
}
