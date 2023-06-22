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
//
////////////////////////////////////////////////////////////////////////////////

// Place fixed size timestamps first to reduce size
const createTablesStmt = `
CREATE TABLE IF NOT EXISTS cache (
	created_at_unix_ms TIMESTAMP NOT NULL,
	expires_at_unix_ms INTEGER NOT NULL,
	key                TEXT PRIMARY KEY NOT NULL,
	data               TEXT NOT NULL,
	UNIQUE(key)        ON CONFLICT REPLACE
) WITHOUT ROWID;`

// const createTablesStmt = `
// CREATE TABLE IF NOT EXISTS cache (
// 	id                 INTEGER PRIMARY KEY,
// 	created_at_unix_ms TIMESTAMP NOT NULL,
// 	expires_at_unix_ms INTEGER NOT NULL,
// 	key                TEXT NOT NULL,
// 	data               TEXT NOT NULL,
// 	UNIQUE(key)        ON CONFLICT REPLACE
// );`

func New(dirname string) (*Cache, error) {
	if err := os.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}
	// TODO: allow callers to specify the filename?
	dbfile := filepath.Join(dirname, "cache.sqlite3")
	// TODO: Auto Vacuum?
	db, err := sql.Open("sqlite3", "file:"+dbfile+
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

func NewUserCache(name string) (*Cache, error) {
	cache, err := userCacheDir()
	if err != nil {
		return nil, err
	}
	return New(cache + string(filepath.Separator) + name)
}

func (c *Cache) Close() error { return c.db.Close() }

func (c *Cache) lazyInit(ctx context.Context) error {
	c.once.Do(func() {
		_, c.err = c.db.ExecContext(ctx, createTablesStmt)
	})
	return c.err
}

func (c *Cache) PutContext(ctx context.Context, key string, v any, ttl time.Duration) error {
	now := time.Now()
	exp := int64(-1)
	if ttl >= 0 {
		exp = now.Add(ttl).UnixMilli()
	}
	data, err := json.Marshal(v)
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

func (c *Cache) Put(key string, v any, ttl time.Duration) error {
	return c.PutContext(context.Background(), key, v, ttl)
}

func isNoRows(err error) bool {
	return err == sql.ErrNoRows || errors.Is(err, sql.ErrNoRows)
}

func (c *Cache) GetContext(ctx context.Context, key string, v any) (bool, error) {
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
	if err := json.Unmarshal(data, v); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Cache) Get(key string, v any) (bool, error) {
	return c.GetContext(context.Background(), key, v)
}

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

func (c *Cache) Prune() error {
	return c.PruneContext(context.Background())
}
