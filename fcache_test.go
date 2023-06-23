package fcache

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
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

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func tempFile(t testing.TB) string {
	return filepath.Join(t.TempDir(), "cache.sqlite3")
}

func TestCache(t *testing.T) {
	c, err := New(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, val := range []int64{1, 2} {
		must(t, c.Store("key", val, -1))

		var i int64
		ok, err := c.Load("key", &i)
		if err != nil {
			t.Fatal(err)
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

func TestEmptyKey(t *testing.T) {
	c, err := New(tempFile(t))
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
	c, err := New(tempFile(t))
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

func TestTTL(t *testing.T) {
	t.Parallel() // Parallel because we sleep
	c, err := New(tempFile(t))
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

	// Sleep until entry is expired
	time.Sleep(time.Millisecond * 20)
	ok, err := c.Load("key2", &i)
	if err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
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

	c, err := New(tempFile(t))
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

	tdelta := func(t1, t2 time.Time) time.Duration {
		d := t1.Sub(t2)
		if d < 0 {
			d *= -1
		}
		return d
	}
	tequal := func(t1, t2 time.Time, delta time.Duration) bool {
		return t1.Round(delta).Equal(t2.Round(delta))
	}

	for i, e1 := range want {
		e2 := ents[i]
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
}

func TestInvalidType(t *testing.T) {
	c, err := New(tempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Store("key", func() {}, -1); err != nil {
		t.Fatalf("%v - %#v", err, err)
	}
}

func TestContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c, err := New(tempFile(t))
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
	c, err := New(tempFile(t))
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

// func TestParallelWrites(t *testing.T) {
// 	// var n int64
// 	tmp := t.TempDir()
// 	n := new(atomic.Int64)
// 	wg := new(sync.WaitGroup)
// 	start := make(chan struct{})
// 	for i := 0; i < 4; i++ {
// 		wg.Add(1)
// 		go func() {
// 			<-start
// 			defer wg.Done()
// 			c, err := New(tmp)
// 			if err != nil {
// 				t.Error(err)
// 				return
// 			}
// 			defer c.Close()
// 			v := n.Add(1)
// 			// if err :=  {
//
// 			// }
// 			if v >= 100_000 {
// 				break
// 			}
// 		}()
// 	}
// }

func TestPrune(t *testing.T) {
	t.Parallel() // Parallel because we sleep
	c, err := New(tempFile(t))
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
	// Sleep until entry is expired
	time.Sleep(time.Millisecond * 20)
	if ok, err := c.Load("key2", &i); err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
	}
	if ok, err := c.Load("key3", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key3", ok, err, true, nil)
	}
}

// TODO: include lazyInit() time?
func BenchmarkCacheNew(b *testing.B) {
	tmp := tempFile(b)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		c, err := New(tmp)
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
	c, err := New(tempFile(b))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

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

func BenchmarkCacheLoad1000(b *testing.B) {
	c, err := New(tempFile(b))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

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
func BenchmarkCacheNewLoad(b *testing.B) {
	tmp := tempFile(b)
	c, err := New(tmp)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	if err := c.Store("key", 1, -1); err != nil {
		b.Fatal(err)
	}
	c.Close()

	for i := 0; i < b.N; i++ {
		cc, err := New(tmp)
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
	c, err := New(tempFile(b))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	if err := c.lazyInit(ctx); err != nil {
		b.Fatal(err)
	}
	const indexStmt = `
	CREATE INDEX
		expires_at_unix_ms_idx
	ON
		cache(expires_at_unix_ms)
	WHERE expires_at_unix_ms >= 0;`
	if _, err := c.db.ExecContext(ctx, indexStmt); err != nil {
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
	baseTTL := time.Duration(time.Now().UnixMilli())
	for i := 0; i < b.N; i++ {
		var ttl time.Duration
		if i&1 == 0 {
			ttl = baseTTL + (time.Duration(i+1) * time.Millisecond * 2)
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
	c, err := New(tempFile(b))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

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

// TODO: test Prune with an expires_at index.
func createExpiresAtUnixIndex(t testing.TB, c *Cache) {
	ctx := context.Background()
	if err := c.lazyInit(ctx); err != nil {
		t.Fatal(err)
	}
	const indexStmt = `
	CREATE INDEX
		expires_at_unix_ms_idx
	ON
		cache(expires_at_unix_ms);`
	if _, err := c.db.ExecContext(ctx, indexStmt); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCachePrune(b *testing.B) {
	c, err := New(tempFile(b))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const N = 1024
	for i := 0; i < N; i++ {
		must(b, c.Store("key_"+strconv.Itoa(i), int64(i), time.Millisecond*time.Duration(i+1)))
	}
	b.ResetTimer()

	for i := int64(0); i < int64(b.N); i++ {
		if err := c.Prune(); err != nil {
			b.Fatal(err)
		}
	}
}
