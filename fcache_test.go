package fcache

import (
	"context"
	"errors"
	"math/rand"
	"os"
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

func TestCache(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, val := range []int64{1, 2} {
		if err := c.Put("key", val, -1); err != nil {
			t.Fatal(err)
		}
		var i int64
		ok, err := c.Get("key", &i)
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

func TestCacheNotFound(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var i int64
	ok, err := c.Get("key", &i)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key", ok, err, false, nil)
	}
}

func TestCacheTTL(t *testing.T) {
	t.Parallel() // Parallel because we sleep
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put("key1", 1, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("key2", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("key3", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Prune(); err != nil {
		t.Fatal(err)
	}
	var i int
	if ok, err := c.Get("key1", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key1", ok, err, true, nil)
	}
	if ok, err := c.Get("key3", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key3", ok, err, true, nil)
	}
	// Sleep until entry is expired
	time.Sleep(time.Millisecond * 20)
	ok, err := c.Get("key2", &i)
	if err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
	}
}

func TestContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.PutContext(ctx, "key", 1, -1); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
	}
	var v int64
	if _, err := c.GetContext(ctx, "key", &v); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
	}
	if err := c.PruneContext(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("want: %v; got: %v", context.Canceled, err)
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
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put("key1", 1, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("key2", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("key3", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Prune(); err != nil {
		t.Fatal(err)
	}
	var i int
	if ok, err := c.Get("key1", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key1", ok, err, true, nil)
	}
	// Sleep until entry is expired
	time.Sleep(time.Millisecond * 20)
	if ok, err := c.Get("key2", &i); err != nil || ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key2", ok, err, false, nil)
	}
	if ok, err := c.Get("key3", &i); err != nil || !ok {
		t.Errorf("Get(%q) = %t, %v; want: %t, %v", "key3", ok, err, true, nil)
	}
}

// TODO: include lazyInit() time?
func BenchmarkCacheNew(b *testing.B) {
	tmp := b.TempDir()
	// ctx := context.Background()
	for i := 0; i < b.N; i++ {
		c, err := New(tmp)
		if err != nil {
			b.Fatal(err)
		}
		// c.lazyInit(ctx)
		c.Close()
	}
}

func BenchmarkCacheGet(b *testing.B) {
	c, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	if err := c.Put("key", 1, -1); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		var i int64
		if ok, err := c.Get("key", &i); err != nil || !ok {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheGet1000(b *testing.B) {
	c, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	rr := rand.New(rand.NewSource(12345))

	// TODO: something like this
	//
	// ids := make([]int16, 1_000)
	// for i := range ids {
	// 	ids[i] = int16(rr.Intn(10_000))
	// }

	// WARN: all these keys are from the beginning
	var keys [8]string
	for i := 0; i < 1_000; i++ {
		key := "key_" + strconv.Itoa(rr.Intn(10_000))
		if i < len(keys) {
			keys[i] = key
		}
		if err := c.Put(key, int64(i), -1); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var v int64
		key := keys[i%len(keys)]
		if ok, err := c.Get(key, &v); err != nil || !ok {
			b.Fatal(err)
		}
	}
}

// Benchmark both opening a new cache and a get. This is a common
// workflow for CLI tools that need to cache short-lived results
// for things like auto-completion.
func BenchmarkCacheNewGet(b *testing.B) {
	tmp := b.TempDir()
	c, err := New(tmp)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	if err := c.Put("key", 1, -1); err != nil {
		b.Fatal(err)
	}
	c.Close()

	for i := 0; i < b.N; i++ {
		cc, err := New(tmp)
		if err != nil {
			b.Fatal(err)
		}
		var i int64
		ok, err := cc.Get("key", &i)
		if !ok || err != nil {
			cc.Close()
			b.Fatal(ok, err)
		}
		cc.Close()
	}
}

func BenchmarkCachePut(b *testing.B) {
	c, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	for i := int64(0); i < int64(b.N); i++ {
		if err := c.Put("key", i, -1); err != nil {
			b.Fatal(err)
		}
	}
}
