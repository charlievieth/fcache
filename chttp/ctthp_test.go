package chttp

import (
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlievieth/fcache"
)

func tempFile(t testing.TB) string {
	return filepath.Join(t.TempDir(), "cache.sqlite3")
}

func testCache(t testing.TB) *fcache.Cache {
	tmp := tempFile(t)
	c, err := fcache.Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Close()
		if t.Failed() {
			t.Logf("Test cache: %s", tmp) // Keep cache on test failure
		} else {
			os.Remove(tmp)
		}
	})
	return c
}

func newTestServerFunc(t testing.TB, useTLS bool, handler func(http.ResponseWriter, *http.Request)) *httptest.Server {
	var srv *httptest.Server
	if useTLS {
		srv = httptest.NewTLSServer(http.HandlerFunc(handler))
	} else {
		srv = httptest.NewServer(http.HandlerFunc(handler))
	}
	t.Cleanup(srv.Close)
	return srv
}

func TestRoundTripper(t *testing.T) {
	var hits atomic.Int32
	var sleep atomic.Int64
	handler := func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		if d := time.Duration(sleep.Load()); d > 0 {
			time.Sleep(d)
		}
		w.Write([]byte("1"))
	}
	srv := newTestServerFunc(t, true, handler)

	client := http.Client{
		Transport: NewRoundTripper(testCache(t), WithRoundTripper(srv.Client().Transport)),
	}

	testResponse := func(t *testing.T, desiredHits int32, addr string) {
		t.Helper()
		res, err := client.Get(addr)
		if err != nil {
			t.Fatal(err)
		}
		data, err := readResponse(res)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "1" {
			t.Errorf("Response = %q; want: %q", data, "1")
		}
		if n := hits.Load(); n != desiredHits {
			t.Errorf("Hits = %d; want: %d", n, desiredHits)
		}
	}

	hits.Store(0)
	testResponse(t, 1, srv.URL)
	testResponse(t, 1, srv.URL)

	t.Run("Parallel", func(t *testing.T) {
		hits.Store(0)
		// addr := srv.URL + "/foo" // WARN: do not do this!!
		sleep.Store(int64(time.Millisecond * 10))
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 5; i++ {
					testResponse(t, 1, srv.URL+"/foo") // WARN
				}
			}()
		}
		close(start)
		wg.Wait()
	})
}

func TestRoundTripperGZip(t *testing.T) {
	const ResponseBody = "hello, world!"
	handler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		gr := gzip.NewWriter(w)
		if _, err := gr.Write([]byte(ResponseBody)); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), 500)
			return
		}
		if err := gr.Close(); err != nil {
			t.Error(err)
			http.Error(w, err.Error(), 500)
			return
		}
	}
	srv := newTestServerFunc(t, true, handler)

	client := http.Client{
		Transport: NewRoundTripper(testCache(t), WithRoundTripper(srv.Client().Transport)),
	}

	for i := 0; i < 3; i++ {
		t.Run("", func(t *testing.T) {
			t.Log("Run:", i)
			req, err := http.NewRequest("GET", srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			// WARN: gzip is only automagically decoded if this is not set
			// req.Header.Set("Accept-Encoding", "gzip")
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, err := readResponse(res)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != ResponseBody {
				t.Errorf("Response = %q; want: %q", data, ResponseBody)
			}
		})
	}
}
