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
	"strconv"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// NB: using an index on "expires_at_unix_ms" makes Prune much faster
// (10x with 2048 rows) but slows down insertion and update by ~30%
// for now the slow down in update/insert is worth the improved Prune
// performance (TBH: this is already way faster than it needs to be
// for the intended use case).
//
// NB: making "expires_at_unix_ms" NULLABLE doesn't help
const createCacheTableStmt = `
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
	expires_at_unix_ms >= 0;`

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

// ReadOnly makes the Cache's database read-only. If the database does not
// already exist, an error is returned.
func ReadOnly() Option {
	return optionFunc(func(c *Cache) {
		c.readOnly = true
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
	readOnly    bool
}

// New returns a new Cache with filename as the SQLite3 database path.
// If filename does not exist, it will be created.
//
// It is allowed, but not always recommended to use ":memory:" as the filename,
// which will create an in-memory cache.
func New(filename string, opts ...Option) (*Cache, error) {
	cache := &Cache{
		busyTimeout: defaultBusyTimeout,
		filename:    filename,
	}
	for _, o := range opts {
		o.apply(cache)
	}
	dsn := url.Values{
		"_busy_timeout": []string{strconv.FormatInt(cache.busyTimeout.Milliseconds(), 10)},
		"_journal_mode": []string{"WAL"},
	}
	if cache.readOnly {
		dsn.Add("mode", "ro")
	}
	db, err := sql.Open("sqlite3", "file:"+filename+"?"+dsn.Encode())
	if err != nil {
		return nil, fmt.Errorf("fcache: %w", err)
	}
	cache.db = db

	// If opened in read-only mode and the file does not exist - better to
	// catch that error here then in a method call.
	//
	// NB: We wait until we've created the DB to perform this check so that
	// we can get its error.
	if cache.readOnly && filename != ":memory:" {
		if fi, err := os.Stat(cache.Database()); err != nil || !fi.Mode().IsRegular() {
			if err := cache.db.Ping(); err != nil {
				cache.db.Close()
				return nil, err
			}
		}
	}
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
// Name is the name of the directory to create under the user cache directory
// and the database name is always "cache.sqlite3".
//
// On Linux this might be "$HOME/.cache/{NAME}/cache.sqlite3".
//
// On macOS this might be "$HOME/Library/Caches/{NAME}/cache.sqlite3".
func NewUserCache(name string, opts ...Option) (*Cache, error) {
	cache, err := userCacheDir()
	if err != nil {
		return nil, fmt.Errorf("fcache: failed to locate user cache directory: %w", err)
	}
	dir := cache + string(os.PathSeparator) + name
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("fcache: failed to create cache directory: %w", err)
	}
	return New(dir+string(os.PathSeparator)+"cache.sqlite3", opts...)
}

// Database returns the file path of the Cache's database.
func (c *Cache) Database() string { return c.filename }

// Close closes the Cache and the underlying database.
func (c *Cache) Close() error { return c.db.Close() }

// isBusyErr returns true is the error is a sqlite3 bust timeout error.
func isBusyErr(err error) bool {
	for err != nil {
		if e, ok := err.(sqlite3.Error); ok {
			return e.Code == sqlite3.ErrBusy
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
			// TODO: The original use case for this library was for it be a very
			// fast cache for CLIs (for example: caching the names of remote
			// servers for bash completion) where the chance of conflicts and
			// busy timeouts is almost zero. In that case an extremely short
			// wait period makes sense. BUT if this library is used / to be
			// used by a longer living program (assuming parallelism) this
			// wait/backoff strategy will be extremely detrimental.any
			//
			// Given the above, we should probably make this tune-able in order
			// to support more use cases.
			tick = time.NewTicker(time.Millisecond)
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

// retry handles BUSY errors
func (c *Cache) retry(ctx context.Context, fn func() error) (err error) {
	if err := c.lazyInit(ctx); err != nil {
		return err
	}
	return c.retryNoInit(ctx, fn)
}

func (c *Cache) retryRows(ctx context.Context, query string) (*sql.Rows, error) {
	var rows *sql.Rows
	err := c.retry(ctx, func() (err error) {
		rows, err = c.db.QueryContext(ctx, query)
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
	c.once.Do(func() {
		err := c.retryNoInit(ctx, func() error {
			// NB(charlie): this is fast enough and any attempt to make it
			// faster has not yielded a significant improvement - so stop.
			_, err := c.db.ExecContext(ctx, createCacheTableStmt)
			return err
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

// StoreContext encodes val to JSON and sets it as the value for key, replacing
// any existing value. If the value cannot be JSON encoded the returned error
// will unwrap to: json.UnsupportedTypeError.
//
// The TTL controls when an entry expires and has millisecond resolution.
// A negative TTL never expires.
func (c *Cache) StoreContext(ctx context.Context, key string, val any, ttl time.Duration) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("fcache: cannot JSON encode (%T) for key (%s): %w",
			val, key, err)
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
	// TODO: use a descriptive error message
	return c.retry(ctx, func() error {
		_, err := c.db.ExecContext(ctx, query, createdAt, expiresAt, key, data)
		return err
	})
}

// Store encodes val to JSON and sets it as the value for key, replacing any
// existing value. If the value cannot be JSON encoded the returned error will
// unwrap to: json.UnsupportedTypeError.
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

// TODO: consider returning sql.ErrNoRows if an entry is not found. That way
// users can distinguish between an expired entry and a missing entry
//
// LoadContext unmarshals the JSON encoded value stored in the cache for key into
// dst and returns true if an entry for key existed and is not expired.
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
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query, key).Scan(&expiredAt, &data)
	})
	if err != nil {
		if isNoRowsErr(err) {
			return false, nil
		}
		return false, err
	}
	if err := c.unmarshal(data, dst); err != nil {
		return false, err
	}
	return expiredAt == -1 || time.Now().UnixMilli() < expiredAt, nil
}

// Load unmarshals the JSON encoded value stored in the cache for key into
// dst and returns true if an entry for key existed and is not expired.
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
	var serr error // scan error
	for rows.Next() {
		var key string
		if serr = rows.Scan(&key); err != nil {
			break
		}
		keys = append(keys, key)
	}
	if serr != nil {
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

// DeleteContext deletes the entry with key from the cache and returns true if
// an entry with key was deleted. If no entry with key exists false is returned.
func (c *Cache) DeleteContext(ctx context.Context, key string) (bool, error) {
	const query = `DELETE FROM cache WHERE key = ?;`
	var found bool
	err := c.retry(ctx, func() error {
		res, err := c.db.ExecContext(ctx, query, key)
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

// Delete deletes the entry with key from the cache and returns true if an entry
// with key was deleted. If no entry with key exists false is returned.
func (c *Cache) Delete(key string) (bool, error) {
	return c.DeleteContext(context.Background(), key)
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

// ExpiredCount return the number of expired entries in the cache.
func (c *Cache) ExpiredCount() (int64, error) {
	return c.ExpiredCountContext(context.Background())
}

// ContainsContext returns true if an entry exists for key. It does not check if
// the entry is expired.
func (c *Cache) ContainsContext(ctx context.Context, key string) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM cache WHERE key = ?);`
	var exists bool
	err := c.retry(ctx, func() error {
		return c.db.QueryRowContext(ctx, query, key).Scan(&exists)
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
	rows, err := c.retryRows(ctx, `SELECT * FROM cache ORDER BY key;`)
	if err != nil {
		return nil, err
	}
	var ents []Entry
	var serr error // scan error
	for rows.Next() {
		var (
			e         Entry
			createdAt int64
			expiresAt int64
		)
		if serr = rows.Scan(&createdAt, &expiresAt, &e.Key, &e.Data); err != nil {
			break
		}
		e.CreatedAt = time.UnixMilli(createdAt)
		if expiresAt >= 0 {
			e.ExpiresAt = time.UnixMilli(expiresAt)
		}
		ents = append(ents, e)
	}
	if serr != nil {
		rows.Close()
		return ents, err
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

// PruneContext prunes any expired entries from the cache and performs a VACUUM
// if any entries were removed.
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
