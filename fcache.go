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
	"strconv"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// TODO: consider storing expires_at_unix_ms as NULL if there is not TTL.

const defaultBusyTimeout = time.Millisecond * 20

// An Option configures a Cache.
type Option interface {
	apply(*Cache)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*Cache)

func (f optionFunc) apply(cache *Cache) {
	f(cache)
}

/*
This routine sets a busy handler that sleeps for a specified amount of time
when a table is locked. The handler will sleep multiple times until at least
"ms" milliseconds of sleeping have accumulated. After at least "ms"
milliseconds of sleeping, the handler returns 0 which causes sqlite3_step() to
return SQLITE_BUSY.

Calling this routine with an argument less than or equal to zero turns off all
busy handlers.
*/

// WithBusyTimeout sets the busy timeout of the Cache.
//
// In addition to SQLite3's busy timeout logic the Cache will also retry queries
// that fail with SQLITE_BUSY until the busy timeout expires. This is to handle
// the busy timeout setting of SQlite3 not always working as expected.
//
// https://www.sqlite.org/pragma.html#pragma_busy_timeout
func BusyTimeout(d time.Duration) Option {
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
	db          *sql.DB
	err         error // init error, if any
	busyTimeout time.Duration
	filename    string
	strictJSON  bool // Disallow unknown fields when unmarshalling JSON
}

// TODO: using an index on "expires_at_unix_ms" makes this much faster
// (10x with 2048 rows) when there are many rows but slows down insertion
// and update by ~30%
//
// TODO: consider making "expires_at_unix_ms" NULLABLE
//
// Place fixed size timestamps first to reduce size
const createCacheTableStmt = `
CREATE TABLE IF NOT EXISTS cache (
	created_at_unix_ms INTEGER NOT NULL,
	expires_at_unix_ms INTEGER NOT NULL,
	key                TEXT PRIMARY KEY NOT NULL,
	data               BLOB NOT NULL,
	UNIQUE(key)        ON CONFLICT REPLACE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS cache_expires_at_unix_ms_idx ON cache(expires_at_unix_ms);`

// New returns a new fcache with database path
func New(database string, opts ...Option) (*Cache, error) {
	cache := &Cache{
		busyTimeout: defaultBusyTimeout,
		filename:    database,
	}
	for _, o := range opts {
		o.apply(cache)
	}
	dsn := url.Values{
		"_busy_timeout": []string{strconv.FormatInt(cache.busyTimeout.Milliseconds(), 10)},
		"_journal_mode": []string{"WAL"},
	}
	db, err := sql.Open("sqlite3", "file:"+database+"?"+dsn.Encode())
	if err != nil {
		return nil, err
	}
	cache.db = db
	return cache, nil
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
		return nil, fmt.Errorf("fcache: failed to locate user cache directory: %w", err)
	}
	dir := cache + string(os.PathSeparator) + name
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("fcache: failed to create cache directory: %w", err)
	}
	return New(dir + string(os.PathSeparator) + "cache.sqlite3")
}

// Close closes the Cache and the underlying database.
func (c *Cache) Close() error { return c.db.Close() }

func isBusyErr(err error) bool {
	e, _ := err.(sqlite3.Error)
	return e.Code == sqlite3.ErrBusy
}

func stopTicker(t *time.Ticker) {
	if t != nil {
		t.Stop()
	}
}

func (c *Cache) retryNoInit(ctx context.Context, fn func() error) (err error) {
	// TODO: initialize the database here to consolidate code
	var tick *time.Ticker
	to := getTimer(c.busyTimeout)
	defer putTimer(to)
	defer stopTicker(tick)
	done := ctx.Done()
Loop:
	for {
		if err = fn(); err == nil || !isBusyErr(err) {
			break
		}
		// Handle busy timeout
		if tick == nil {
			// TODO: use an exponential backoff
			tick = time.NewTicker(time.Millisecond)
		}
		select {
		case <-tick.C:
			// Ok
		case <-done:
			err = ctx.Err()
			break Loop
		case <-to.C:
			break Loop
		}
	}
	return err
}

// retry handles BUSY errors
func (c *Cache) retry(ctx context.Context, fn func() error) (err error) {
	if err := c.lazyInit(ctx); err != nil {
		return err
	}
	return c.retryNoInit(ctx, fn)
}

func (c *Cache) lazyInit(ctx context.Context) error {
	c.once.Do(func() {
		c.err = c.retryNoInit(ctx, func() error {
			// NB(charlie): this is fast enough and any attempt to make it
			// faster has not yielded a significant improvement - so stop.
			if _, err := c.db.ExecContext(ctx, createCacheTableStmt); err != nil {
				return fmt.Errorf("fcache: failed to initialize database (%s): %w",
					c.filename, err)
			}
			return nil
		})
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
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("fcache: failed to marshal value (%T) for key (%s): %w",
			val, key, err)
	}
	now := time.Now()
	exp := int64(-1)
	if ttl >= 0 {
		exp = now.Add(ttl).UnixMilli()
	}
	const query = `
	INSERT INTO cache (
		created_at_unix_ms,
		expires_at_unix_ms,
		key,
		data
	) VALUES (?, ?, ?, ?);`
	// TODO: use a descriptive error message
	return c.retry(ctx, func() error {
		_, err := c.db.ExecContext(ctx, query, now.UnixMilli(), exp, key, data)
		return err
	})
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

// LoadContext loads the value stored in the cache for key and, if found and
// the entry has not expired, stores it in dst. LoadContext returns true if
// the value associated with key was found and not expired.
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
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query, key).Scan(&expiredAt, &data)
	})
	if err != nil {
		if isNoRowsErr(err) {
			return false, nil
		}
		return false, err
	}
	if expiredAt != -1 && expiredAt < time.Now().UnixMilli() {
		return false, nil
	}
	if err := c.unmarshal(data, dst); err != nil {
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

// CountContext returns the number of entries in the cache.
func (c *Cache) CountContext(ctx context.Context) (int64, error) {
	const query = `SELECT COUNT(*) FROM cache;`
	var n int64
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query).Scan(&n)
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
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query, ts).Scan(&n)
	})
	return n, err
}

// ExpiredCountContext return the number of expired entries in the cache.
func (c *Cache) ExpiredCount() (int64, error) {
	return c.ExpiredCountContext(context.Background())
}

// func (c *Cache) ContainsContext(ctx context.Context, key string) (bool, error) {
// 	const query = `SELECT EXISTS(SELECT 1 FROM cache WHERE key = ?);`
// 	var exists bool
// 	err := c.retry(ctx, func() error {
// 		return c.db.QueryRowContext(ctx, query, key).Scan(&exists)
// 	})
// 	return exists, err
// }

// Entry returns the Entry for key or ErrNoRows if it does not exist.
func (c *Cache) EntryContext(ctx context.Context, key string) (*Entry, error) {
	const query = `SELECT * FROM cache WHERE key = ? LIMIT 1;`
	var (
		e         Entry
		createdAt int64
		expiresAt int64
	)
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query, key).Scan(&createdAt, &expiresAt, &e.Key, &e.Data)
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

// EntriesContext returns a slice of all the entries in the cache.
func (c *Cache) EntriesContext(ctx context.Context) ([]Entry, error) {
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
	// TODO: check if the database exists
	const query = `
	DELETE FROM
		cache
	WHERE
		0 <= expires_at_unix_ms AND expires_at_unix_ms < ?;`
	return c.retry(ctx, func() error {
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
	})
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

// TTL returns the TTL the Entry was created with or -1 if the Entry does not
// have a TTL.
func (e *Entry) TTL() time.Duration {
	if e.ExpiresAt.IsZero() {
		return -1
	}
	return e.ExpiresAt.Sub(e.CreatedAt)
}

// Unmarshal is a helper to unmarshal the data stored in the Entry into v.
func (e *Entry) Unmarshal(v any) error {
	return json.Unmarshal(e.Data, &v)
}

// HasTTL returns true if the Entry was created with a TTL.
func (e *Entry) HasTTL() bool {
	return !e.ExpiresAt.IsZero()
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
