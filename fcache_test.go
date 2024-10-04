package fcache

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func TestUserCacheDir(t *testing.T) {
	t.Run("XDG", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("XDG_CACHE_HOME", tmp)
		dir, err := userCacheDir()
		if err != nil {
			t.Fatal(err)
		}
		if dir != tmp {
			t.Errorf("got: %q; want: %q", dir, tmp)
		}
	})

	t.Run("HOME", func(t *testing.T) {
		t.Setenv("XDG_CACHE_HOME", "")
		d1, e1 := userCacheDir()
		d2, e2 := os.UserCacheDir()
		if e1 != e2 || d1 != d2 {
			t.Errorf("userCacheDir() = %s, %v; want: %s, %v", d1, e1, d2, e2)
		}
	})
}

func TestOpenUserCache(t *testing.T) {
	tmp := t.TempDir()
	// dir := filepath.Join(tmp)
	t.Setenv("XDG_CACHE_HOME", tmp)
	c, err := OpenUserCache("test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	must(t, c.Store("key", 1, -1))

	dbfile := filepath.Join(tmp, "fcache", "test", "cache.sqlite3")
	if _, err := os.Stat(dbfile); err != nil {
		t.Fatal(err)
	}
}

// Allow for sub-caches like "my_project/{cache_1,cache_2}"
func TestOpenUserCacheSubCache(t *testing.T) {
	tmp := t.TempDir()
	// dir := filepath.Join(tmp)
	t.Setenv("XDG_CACHE_HOME", tmp)
	c, err := OpenUserCache("parent/child")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	must(t, c.Store("key", 1, -1))

	dbfile := filepath.Join(tmp, "fcache", "parent", "child", "cache.sqlite3")
	if _, err := os.Stat(dbfile); err != nil {
		t.Fatal(err)
	}
}

func TestSpaceInDatabaseName(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(tmp)
	})
	c, err := Open(filepath.Join(tmp, "my database name.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	must(t, c.Store("k", 1, -1))
	var v int64
	if _, err := c.Load("k", &v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("got: %d; want: %d", v, 1)
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%[1]s - %#[1]v", err)
	}
}

func tempFile(t testing.TB) string {
	tmp, err := os.MkdirTemp("", "fcache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			if err := os.RemoveAll(tmp); err != nil {
				t.Error(err)
			}
			return
		}
		t.Log("TempDir:", tmp)
	})
	return filepath.Join(tmp, "cache.sqlite3")
}

func TestCache(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, val := range []int64{1, 2} {
		must(t, c.Store("key", val, -1))

		var i int64
		ok, err := c.Load("key", &i)
		if err != nil {
			t.Fatalf("%[1]s - %#[1]v", err)
		}
		if !ok {
			t.Errorf("failed to find key: %q", "key")
			continue
		}
		if i != val {
			t.Errorf("got: %d want: %d", i, 1)
		}
	}
}

func TestCacheBusyTimeout(t *testing.T) {
	c, err := Open(tempFile(t), WithBusyTimeout(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 10; i++ {
		must(t, c.Store("key"+strconv.Itoa(i), 1, -1))
	}
}

func TestSchemaTable(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	must(t, c.Store("key", 1, -1)) // Init tables

	const existsQuery = `SELECT EXISTS(
		SELECT name FROM sqlite_master WHERE type='table' AND name='schema'
	);`

	var exists bool
	if err := c.writeDB.QueryRow(existsQuery).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Failed to create schema table")
	}

	var schema int64
	if err := c.writeDB.QueryRow("SELECT max(version) FROM schema;").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if schema != schemaVersion {
		t.Fatalf("schema = %d; want: %d", schema, schemaVersion)
	}
}

// Wait for the clock to advance one millisecond
func waitMilli(t testing.TB) {
	ts := time.Now().UnixMilli()
	for i := 0; i < 50; i++ {
		if time.Now().UnixMilli() != ts {
			return
		}
		time.Sleep(time.Millisecond / 2)
	}
	t.Fatal("failed to wait for clock to advance by one millisecond")
}

func TestCount(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	assertCount := func(want int64) {
		n, err := c.Count()
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("Count() = %d; want: %d", n, want)
		}
	}

	for i := 0; i < 8; i++ {
		must(t, c.Store("key_"+strconv.Itoa(i), i, 1))
	}
	assertCount(8)

	waitMilli(t)
	if err := c.Prune(); err != nil {
		t.Fatal(err)
	}
	assertCount(0)
}

func TestEmptyKey(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	must(t, c.Store("", int64(1), -1))

	var i int64
	ok, err := c.Load("", &i)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("failed to find key: %q", "")
	}
	if i != 1 {
		t.Errorf("got: %d want: %d", i, 1)
	}
}

func TestKeyNotFound(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var i int64
	ok, err := c.Load("key", &i)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key", ok, err, false, nil)
	}
}

func TestExpiredCount(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := []string{"key1", "key2"}
	for _, k := range keys {
		must(t, c.Store(k, k, -1))
	}
	if n, err := c.ExpiredCount(); err != nil || n != 0 {
		t.Errorf("ExpiredCount() = %d, %v; want: %d, %v", n, err, 0, nil)
	}

	for _, k := range keys {
		must(t, c.Store(k, k, 0))
	}
	waitMilli(t)
	if n, err := c.ExpiredCount(); err != nil || n != 2 {
		t.Errorf("ExpiredCount() = %d, %v; want: %d, %v", n, err, 2, nil)
	}
}

func TestContains(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := []string{"key1", "key2"}
	for _, k := range keys {
		must(t, c.Store(k, k, -1))
		if ok, err := c.Contains(k); !ok || err != nil {
			t.Errorf("Contains(%q) = %t, %v; want: %t, %v", k, ok, err, true, nil)
		}
	}

	// Test with expired entries
	for _, k := range keys {
		must(t, c.Store(k, k, 0))
	}
	waitMilli(t)
	for _, k := range keys {
		must(t, c.Store(k, k, -1))
		if ok, err := c.Contains(k); !ok || err != nil {
			t.Errorf("Contains(%q) = %t, %v; want: %t, %v", k, ok, err, true, nil)
		}
	}

	// Non-existent entry
	if ok, err := c.Contains("not_found"); ok || err != nil {
		t.Errorf("Contains(%q) = %t, %v; want: %t, %v", "not_found", ok, err, false, nil)
	}
}

func TestKeys(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := make([]string, 64)
	for i := range keys {
		keys[i] = "key_" + strconv.Itoa(i)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i, k := range keys {
		must(t, c.Store(k, i, -1))
	}

	got, err := c.Keys()
	if err != nil {
		t.Fatal(err)
	}
	if !sort.StringsAreSorted(got) {
		t.Error("keys are not sorted")
	}
	if len(got) != len(keys) {
		t.Fatalf("len(got) = %d; want: %d", len(got), len(keys))
	}
	sort.Strings(keys)
	for i := range got {
		if got[i] != keys[i] {
			t.Errorf("got[%d] = %q; want: %q", i, got[i], keys[i])
		}
	}
}

func TestDelete(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	keys := []string{"key1", "key2"}
	for _, k := range keys {
		must(t, c.Store(k, k, -1))
	}

	// Found
	if ok, err := c.Delete("key1"); !ok || err != nil {
		t.Errorf("Delete(%q) = %t, %v; want: %t, %v", "key1", ok, err, true, nil)
	}
	if ok, err := c.Contains("key1"); ok || err != nil {
		t.Errorf("Contains(%q) = %t, %v; want: %t, %v", "key1", ok, err, false, nil)
	}

	// Not found
	if ok, err := c.Delete("not_found"); ok || err != nil {
		t.Errorf("Delete(%q) = %t, %v; want: %t, %v", "not_found", ok, err, false, nil)
	}
}

func assertCount(t *testing.T, c *Cache, want int) (passed bool) {
	t.Helper()
	n, err := c.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(want) {
		t.Fatalf("Count() = %d; want: %d", n, want)
	}
	return n == int64(want)
}

func TestDeleteMany(t *testing.T) {
	test := func(t *testing.T, count int) {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			c, err := Open(tempFile(t))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			var keys []string
			for i := 0; i < count; i++ {
				keys = append(keys, strconv.Itoa(i))
			}
			for _, k := range keys {
				must(t, c.Store(k, k, -1))
			}
			assertCount(t, c, count)

			// Special case of zero keys
			if len(keys) == 0 {
				if ok, err := c.DeleteMany(keys...); ok || err != nil {
					t.Errorf("DeleteMany(%q) = %t, %v; want: %t, %v", keys, ok, err, false, nil)
				}
				return
			}

			// Found
			if ok, err := c.DeleteMany(keys...); !ok || err != nil {
				t.Errorf("DeleteMany(%q) = %t, %v; want: %t, %v", keys, ok, err, true, nil)
			}
			for _, k := range keys {
				if ok, err := c.Contains(k); ok || err != nil {
					t.Errorf("Contains(%q) = %t, %v; want: %t, %v", k, ok, err, false, nil)
				}
			}
			assertCount(t, c, 0)

			// Not found
			if ok, err := c.DeleteMany(keys...); ok || err != nil {
				t.Errorf("DeleteMany(%q) = %t, %v; want: %t, %v", keys, ok, err, false, nil)
			}
		})
	}
	for _, n := range []int{0, 1, 2, 4, 16} {
		test(t, n)
	}
}

func TestDisallowUnknownFields(t *testing.T) {
	c, err := Open(tempFile(t), DisallowUnknownFields())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	must(t, c.Store("key", map[string]int{"A": 1, "B": 2, "C": 3, "D": 4}, -1))

	var dst struct {
		A, B, C int
	}
	if _, err := c.Load("key", &dst); err == nil {
		t.Error("Expected a \"json: unknown field\" error")
	}
}

// WARN: removing this since readonly does not work when creating
// the schema table since needs to insert the initial schema value.
// func TestReadOnly(t *testing.T) {
// 	tmp := tempFile(t)
// 	c, err := Open(tmp)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer c.Close()
//
// 	keys := []string{"key1", "key2"}
// 	for _, k := range keys {
// 		must(t, c.Store(k, k, -1))
// 	}
//
// 	if err := c.Close(); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	openReadOnly := func(t *testing.T) *Cache {
// 		c, err := Open(tmp, ReadOnly())
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		t.Cleanup(func() { c.Close() })
// 		return c
// 	}
//
// 	t.Run("Read", func(t *testing.T) {
// 		c := openReadOnly(t)
// 		for _, k := range keys {
// 			var dst string
// 			if ok, err := c.Load(k, &dst); !ok || err != nil {
// 				t.Errorf("Load(%q, %T) = %t, %v; want: %t, %v", k, dst, ok, err, true, nil)
// 			}
// 		}
// 	})
//
// 	t.Run("Write", func(t *testing.T) {
// 		err := openReadOnly(t).Store("new_key", "1", -1)
// 		var target sqlite3.Error
// 		if !errors.As(err, &target) {
// 			t.Fatalf("Expected error type: %T; got: %T", target, err)
// 		}
// 		if target.Code != sqlite3.ErrReadonly {
// 			t.Fatalf("Expected error code: %v; got: %v", sqlite3.ErrReadonly, target.Code)
// 		}
// 	})
//
// 	t.Run("Open", func(t *testing.T) {
// 		c, err := Open(tempFile(t), ReadOnly())
// 		t.Cleanup(func() {
// 			if c != nil {
// 				c.Close()
// 			}
// 		})
// 		if err == nil {
// 			t.Fatalf("Expected error got: %v", err)
// 		}
// 	})
// }

func TestTTL(t *testing.T) {
	t.Parallel() // Parallel because we sleep
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	must(t, c.Store("key1", 1, -1))
	must(t, c.Store("key2", 1, 0))
	must(t, c.Store("key3", 1, time.Minute))
	must(t, c.Prune())

	var i int
	if ok, err := c.Load("key1", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key1", ok, err, true, nil)
	}
	if ok, err := c.Load("key3", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key3", ok, err, true, nil)
	}

	waitMilli(t) // sleep until entry is expired
	ok, err := c.Load("key2", &i)
	if err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
	}
}

func TestInvalidType(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Store("key", func() {}, -1)
	if err == nil {
		t.Fatal("expected an error but got none")
	}
	var dst *json.UnsupportedTypeError
	if !errors.As(err, &dst) {
		t.Fatalf("invalid error type: %T", err)
	}
}

func TestNullValues(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	t.Run("Store", func(t *testing.T) {
		if err := c.Store("key2", nil, -1); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("LoadPointer", func(t *testing.T) {
		var v *struct{}
		_, err := c.Load("key2", &v)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("v == %+v; want: nil", v)
		}
	})

	t.Run("LoadValue", func(t *testing.T) {
		var v struct{}
		_, err := c.Load("key2", &v)
		if err != nil {
			t.Fatal(err)
		}
		if v != struct{}{} {
			t.Errorf("v == %+v; want: %+v", v, struct{}{})
		}
	})

	t.Run("NullDest", func(t *testing.T) {
		// TODO: return a useful error here
		_, err := c.Load("key2", nil)
		if err == nil {
			t.Fatal(err)
		}
		var target *json.InvalidUnmarshalError
		if !errors.As(err, &target) {
			t.Fatalf("expected error (%#[1]v) to unwrap to: %[2]T: %#[2]v", err, target)
		}
	})
}

func TestContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.StoreContext(ctx, "key", 1, -1); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
	}
	var v int64
	if _, err := c.LoadContext(ctx, "key", &v); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
	}
	if err := c.PruneContext(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
	}
}

func TestClosed(t *testing.T) {
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Store("key", 1, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	var v int64
	if _, err := c.Load("key", &v); err == nil {
		t.Fatal("expected error after closing the cache")
	}
}

func TestParallelWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: short test")
	}
	keys := new([8192]string)
	for i := range keys {
		keys[i] = "key_" + strconv.Itoa(i)
	}

	tmp := tempFile(t)
	n := new(atomic.Int64)
	wg := new(sync.WaitGroup)
	start := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel context on error
	errorf := func(format string, args ...any) {
		t.Helper()
		t.Errorf(format, args...)
		cancel()
	}

	numCPU := 4
	if runtime.NumCPU() == 1 {
		numCPU = 2
	}
	c, err := Open(tmp)
	if err != nil {
		errorf("%v", err)
		return
	}
	defer c.Close()
	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for v := n.Add(1); v < 100_000; v = n.Add(1) {
				switch {
				case v%512 == 0:
					if err := c.PruneContext(ctx); err != nil {
						errorf("%d.%d: %v - %#v", id, v, err, err)
						return
					}
				case v&1 != 0:
					key := keys[v%int64(len(keys))]
					if err := c.StoreContext(ctx, key, v, time.Millisecond*5); err != nil {
						errorf("%d.%d: %v - %#v", id, v, err, err)
						return
					}
				default:
					key := keys[v%int64(len(keys))]
					var dst int64
					if _, err := c.LoadContext(ctx, key, &dst); err != nil {
						errorf("%d.%d: %v - %#v", id, v, err, err)
						return
					}
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}

func TestPrune(t *testing.T) {
	t.Parallel() // Parallel because we sleep
	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Store("key1", 1, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.Store("key2", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Store("key3", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Prune(); err != nil {
		t.Fatal(err)
	}
	var i int
	if ok, err := c.Load("key1", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key1", ok, err, true, nil)
	}

	waitMilli(t) // sleep until entry is expired
	if ok, err := c.Load("key2", &i); err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
	}
	if ok, err := c.Load("key3", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key3", ok, err, true, nil)
	}
}

func TestEntries(t *testing.T) {
	toJSON := func(v any) []byte {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	c, err := Open(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	now := time.Now()
	expiresAt := now.Add(time.Minute).Round(time.Millisecond)
	want := []Entry{
		{
			Key:       "key1",
			CreatedAt: now,
			ExpiresAt: time.Time{},
			Data:      toJSON(123),
		},
		{
			Key:       "key2",
			CreatedAt: now,
			ExpiresAt: expiresAt,
			Data:      toJSON(map[int]int{1: 1, 2: 2, 3: 3}),
		},
		{
			Key:       "key3",
			CreatedAt: now,
			ExpiresAt: expiresAt,
			Data:      toJSON(struct{ a, b, c int }{11, 22, 33}),
		},
	}

	// Insert in reverse order to make sure we sort by key
	for i := len(want) - 1; i >= 0; i-- {
		e := want[i]
		must(t, c.Store(e.Key, e.Data, e.TTL()))
	}

	ents, err := c.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != len(want) {
		t.Fatalf("Got %d entries; want: %d", len(ents), len(want))
	}

	entriesEqual := func(i int, e1, e2 *Entry) {
		tdelta := func(t1, t2 time.Time) time.Duration {
			d := t1.Sub(t2)
			if d < 0 {
				d *= -1
			}
			return d
		}
		tequal := func(t1, t2 time.Time, delta time.Duration) bool {
			return tdelta(t1, t2) <= delta
		}
		if e2.Key != e1.Key {
			t.Errorf("%d: Key: got %q; want: %q", i, e2.Key, e1.Key)
		}
		if string(e2.Data) != string(e1.Data) {
			t.Errorf("%d: Data: got %s; want: %s", i, e2.Data, e1.Data)
		}
		if !tequal(e2.ExpiresAt, e1.ExpiresAt, time.Millisecond*100) {
			t.Errorf("%d: ExpiresAt: got %s; want: %s; delta: %s",
				i, e2.ExpiresAt, e1.ExpiresAt, tdelta(e2.ExpiresAt, e1.ExpiresAt))
		}
		if !tequal(e2.CreatedAt, e1.CreatedAt, time.Millisecond*100) {
			t.Errorf("%d: CreatedAt: got %s; want: %s; delta: %s",
				i, e2.CreatedAt, e1.CreatedAt, tdelta(e2.CreatedAt, e1.CreatedAt))
		}
	}

	for i := range want {
		entriesEqual(i, &want[i], &ents[i])
	}

	for i := range want {
		got, err := c.Entry(want[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		entriesEqual(i, &want[i], got)
	}
}

func TestEntryExpired(t *testing.T) {
	now := time.Now()
	e := Entry{CreatedAt: now, ExpiresAt: time.Time{}}
	if e.Expired() {
		t.Errorf("%+v.Expired() = %t; want: %t", e, e.Expired(), false)
	}
	e.ExpiresAt = now.Add(time.Second)
	if e.Expired() {
		t.Errorf("%+v.Expired() = %t; want: %t", e, e.Expired(), false)
	}
	e.ExpiresAt = now.Add(-time.Second)
	if !e.Expired() {
		t.Errorf("%+v.Expired() = %t; want: %t", e, e.Expired(), true)
	}
}

func TestEntryTTL(t *testing.T) {
	now := time.Now()
	e := Entry{CreatedAt: now, ExpiresAt: now.Add(time.Second)}
	if e.TTL() != time.Second {
		t.Errorf("TTL = %s; want: %s", e.TTL(), time.Second)
	}
}

func TestEntryUnmarshal(t *testing.T) {
	e := Entry{Data: json.RawMessage(`"abc"`)}
	var s string
	if err := e.Unmarshal(&s); err != nil {
		t.Fatal(err)
	}
	if s != "abc" {
		t.Errorf("got: %q; want: %q", s, "abc")
	}
}

func TestIsRetryableError(t *testing.T) {
	for _, err := range []error{
		sqlite3.Error{Code: sqlite3.ErrBusy},
		sqlite3.Error{Code: sqlite3.ErrLocked},
		// Handle the error being wrapped as a pointer
		&sqlite3.Error{Code: sqlite3.ErrBusy},
		&sqlite3.Error{Code: sqlite3.ErrLocked},
	} {
		if ok := isRetryableErr(err); !ok {
			t.Errorf("isRetryableErr(%#v) = %t; want: %t", err, ok, true)
		}
		for i := 0; i < 3; i++ {
			err = fmt.Errorf("wrapped %d: %w", i, err)
			if ok := isRetryableErr(err); !ok {
				t.Errorf("isRetryableErr(%#v) = %t; want: %t", err, ok, true)
			}
		}
	}
}

var memory = flag.Bool("memory", false, "Use in memory database for benchmarks")

func benchDatabase(t testing.TB) string {
	if *memory {
		return ":memory:"
	}
	return filepath.Join(t.TempDir(), "test.sqlite3")
}

func benchCache(t testing.TB, opts ...Option) *Cache {
	c, err := Open(benchDatabase(t), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func BenchmarkCacheOpen(b *testing.B) {
	tmp := benchDatabase(b)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		c, err := Open(tmp)
		if err != nil {
			b.Fatal(err)
		}
		if err := c.lazyInit(ctx); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func BenchmarkCacheLoad(b *testing.B) {
	c := benchCache(b)

	if err := c.Store("key", 1, -1); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		var i int64
		if ok, err := c.Load("key", &i); err != nil || !ok {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheLoadParallel(b *testing.B) {
	c := benchCache(b)

	if err := c.Store("key", 1, -1); err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var i int64
			if ok, err := c.Load("key", &i); err != nil || !ok {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCacheLoad1000(b *testing.B) {
	c := benchCache(b)

	rr := rand.New(rand.NewSource(12345))

	ids := make([]int32, 1_000)
	for i := range ids {
		ids[i] = int32(rr.Int31())
	}

	for i, n := range ids {
		key := "key_" + strconv.Itoa(int(n))
		if err := c.Store(key, int64(i), -1); err != nil {
			b.Fatal(err)
		}
	}

	// Randomly pick 8 valid keys
	var keys [8]string
	rr.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	for i := 0; i < len(keys); i++ {
		keys[i] = "key_" + strconv.Itoa(int(ids[i]))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var v int64
		key := keys[i%len(keys)]
		if ok, err := c.Load(key, &v); err != nil || !ok {
			b.Fatal(err)
		}
	}
}

// Benchmark both opening a new cache and a get. This is a common
// workflow for CLI tools that need to cache short-lived results
// for things like auto-completion.
func BenchmarkCacheOpenLoad(b *testing.B) {
	if *memory {
		b.Skip("skipping: benchmark cannot run with the 'memory' option")
	}
	tmp := tempFile(b)
	c, err := Open(tmp)
	if err != nil {
		b.Fatal(err)
	}

	if err := c.Store("key", 1, -1); err != nil {
		c.Close()
		b.Fatal(err)
	}
	if err := c.Close(); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		cc, err := Open(tmp)
		if err != nil {
			b.Fatal(err)
		}
		var i int64
		ok, err := cc.Load("key", &i)
		if !ok || err != nil {
			cc.Close()
			b.Fatal(ok, err)
		}
		cc.Close()
	}
}

func BenchmarkCacheStore(b *testing.B) {
	c := benchCache(b)

	ctx := context.Background()
	if err := c.lazyInit(ctx); err != nil {
		b.Fatal(err)
	}

	var keys = [8]string{
		"key_1",
		"key_2",
		"key_3",
		"key_4",
		"key_5",
		"key_6",
		"key_7",
		"key_8",
	}
	for i := 0; i < b.N; i++ {
		var ttl time.Duration
		if i&1 == 0 {
			ttl = time.Minute
		} else {
			ttl = -1
		}
		key := keys[i%len(keys)]
		if err := c.Store(key, int64(i), ttl); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheEntries(b *testing.B) {
	c := benchCache(b)

	var keys = [8]string{
		"key_1",
		"key_2",
		"key_3",
		"key_4",
		"key_5",
		"key_6",
		"key_7",
		"key_8",
	}
	for _, key := range keys {
		must(b, c.Store(key, int64(1), time.Minute))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ents, err := c.Entries()
		if err != nil {
			b.Fatal(err)
		}
		_ = ents
	}
}

func BenchmarkCachePrune(b *testing.B) {
	c := benchCache(b)

	const N = 1024
	for i := 0; i < N; i++ {
		must(b, c.Store("key_"+strconv.Itoa(i), int64(i), time.Hour))
	}
	b.ResetTimer()

	for i := int64(0); i < int64(b.N); i++ {
		if err := c.Prune(); err != nil {
			b.Fatal(err)
		}
	}
}
