package chttp

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/charlievieth/fcache"
	"golang.org/x/sync/singleflight"
)

const (
	// TODO:
	// 	* Header when the response is served from the cache
	// 	* Header to delete any cached responses
	CacheHeader   = "x-fcache-cache"
	NoCacheHeader = "x-fcache-no-cache"
	TTLHeader     = "x-fcache-ttl"

	canonicalCacheHeader   = "X-Fcache-Cache"
	canonicalNoCacheHeader = "X-Fcache-No-Cache"
	canonicalTTLHeader     = "X-Fcache-Ttl"

	DefaultTTL = time.Minute
	// InvalidTTL = -1 << 63 // math.MinInt64
)

func SetCacheHeader(hdr http.Header) {
	hdr.Del(canonicalNoCacheHeader)
	hdr.Set(canonicalCacheHeader, "1")
}

func SetNoCacheHeader(hdr http.Header) {
	hdr.Del(canonicalCacheHeader)
	hdr.Set(canonicalNoCacheHeader, "1")
}

func SetTTLHeader(hdr http.Header, ttl time.Duration) {
	ms := int64(-1)
	if ttl >= 0 {
		ms = ttl.Milliseconds()
	}
	SetCacheHeader(hdr)
	hdr.Set(canonicalTTLHeader, strconv.FormatInt(ms, 10))
}

func GetCacheHeader(hdr http.Header) bool {
	ok, _ := strconv.ParseBool(hdr.Get(canonicalCacheHeader))
	return ok
}

func GetNoCacheHeader(hdr http.Header) bool {
	ok, _ := strconv.ParseBool(hdr.Get(canonicalNoCacheHeader))
	return ok
}

func GetTTLHeader(hdr http.Header) (time.Duration, bool) {
	if s := hdr.Get(canonicalTTLHeader); s != "" {
		ms, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return time.Duration(ms) * time.Millisecond, true
	}
	return 0, false
}

type Option interface {
	apply(*RoundTripper)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*RoundTripper)

func (f optionFunc) apply(rt *RoundTripper) {
	f(rt)
}

func WithRoundTripper(rt http.RoundTripper) Option {
	return optionFunc(func(r *RoundTripper) {
		r.rt = rt
	})
}

func WithDefaultTTL(ttl time.Duration) Option {
	return optionFunc(func(r *RoundTripper) {
		if ttl < 0 {
			ttl = -1
		}
		r.baseTTL = ttl
	})
}

type RoundTripper struct {
	cache           *fcache.Cache
	rt              http.RoundTripper
	group           singleflight.Group
	baseTTL         time.Duration
	invalidateCache bool
	// TODO: rename
	includeHeaders bool // include request headers in the cache key
	// compressResponse bool
	// excludedHeaders []string // headers to exclude in the cache key
	//
	// TODO: use this or a function
	// includeHeaders map[string]struct{} // headers to include in the cache key
}

func NewRoundTripper(cache *fcache.Cache, opts ...Option) *RoundTripper {
	rt := &RoundTripper{
		cache:   cache,
		rt:      http.DefaultTransport,
		baseTTL: DefaultTTL,
	}
	for _, opt := range opts {
		opt.apply(rt)
	}
	return rt
}

// WARN: respect headers
func (r *RoundTripper) cachable(req *http.Request) bool {
	if req.Method != "GET" {
		return false
	}
	if s := req.Header.Get(NoCacheHeader); s != "" {

	}
	nocache, _ := strconv.ParseBool(req.Header.Get(NoCacheHeader))
	return !nocache
}

func (r *RoundTripper) shouldInvalidate(req *http.Request) bool {
	switch req.Method {
	case "PUT", "POST", "DELETE", "PATCH":
		return true
	}
	return false
}

func readAllSize(r io.Reader, size int64) ([]byte, error) {
	var n int
	if size > 0 && int64(int(size)) == size {
		n = int(size)
	}
	n++ // one byte for final read at EOF
	if n < bytes.MinRead {
		n = bytes.MinRead
	}
	b := make([]byte, 0, n)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

func readResponse(res *http.Response) ([]byte, error) {
	return readAllSize(res.Body, res.ContentLength)
}

type header struct {
	key, value string
}

// getHost tries its best to return the request host.
// According to section 14.23 of RFC 2616 the Host header
// can include the port number if the default value of 80 is not used.
func getHost(r *http.Request) string {
	if r.URL.IsAbs() {
		return r.URL.Host
	}
	return r.Host
}

func (r *RoundTripper) cacheKey(req *http.Request) string {
	// host := getHost(req)
	url := "GET:" + req.URL.String()
	if r.includeHeaders {
		h := sha256.New()
		req.Header.Write(h)
		url += ":" + hex.EncodeToString(h.Sum(nil))[:64]
	}
	return url
}

func (r *RoundTripper) tryCache(key string, req *http.Request) (*http.Response, error) {
	var res cachedResponse
	ok, err := r.cache.Load(key, &res)
	if err != nil {
		return nil, err
	}
	if !ok {
		// WARN WARN WARN WARN
		return nil, nil // WARN: nil, nil is bad
	}
	rr := &http.Response{
		Status:           http.StatusText(res.StatusCode),
		StatusCode:       res.StatusCode,
		Proto:            res.Proto,
		ProtoMajor:       res.ProtoMajor,
		ProtoMinor:       res.ProtoMinor,
		Header:           res.Header, // WARN: careful including all headers
		Body:             io.NopCloser(bytes.NewReader(res.Body)),
		ContentLength:    res.ContentLength,
		TransferEncoding: res.TransferEncoding,
		Close:            res.Close,
		Uncompressed:     res.Uncompressed,
		Trailer:          res.Trailer,
	}
	return rr, nil
}

// WARN: don't do this - instead check the Uncompressed field and headers !!!
func isGzipd(data []byte) bool {
	_, err := gzip.NewReader(bytes.NewReader(data))
	return err == nil
}

func (r *RoundTripper) cacheResponse(res *http.Response, key string, data []byte) error {
	// TODO: check header for this ALSO check if the net/http
	// library is/will automatically handly GZip for us.
	if !isGzipd(data) {
		var w bytes.Buffer
		wr := gzip.NewWriter(&w)
		if _, err := wr.Write(data); err != nil {
			return err
		}
		if err := wr.Close(); err != nil {
			return err
		}
		data = w.Bytes()
	}
	return r.cache.Store(key, newCachedResponse(res, data), -1)
}

func (r *RoundTripper) invalidate(req *http.Request, key string) (err error) {
	if r.shouldInvalidate(req) {
		_, err = r.cache.DeleteContext(req.Context(), key)
	}
	return err
}

// TODO: should we return an error to report invalid TTLs ???
func (r *RoundTripper) getTTL(req *http.Request) time.Duration {
	if ttl, ok := GetTTLHeader(req.Header); ok {
		return ttl
	}
	return r.baseTTL
}

func (r *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// TODO: handle parallel requests
	if !r.cachable(req) {
		if err := r.invalidate(req, r.cacheKey(req)); err != nil {
			return nil, err
		}
		return r.rt.RoundTrip(req)
	}
	key := r.cacheKey(req)
	// WARN: do this in the single flight group!
	switch res, err := r.tryCache(key, req); {
	case err != nil:
		return nil, err
	case res != nil:
		return res, nil
	}
	type response struct {
		res  *http.Response
		body []byte
	}
	v, err, shared := r.group.Do(key, func() (any, error) {
		res, err := r.rt.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		body, err := readResponse(res)
		if err != nil {
			return &response{res, body}, err
		}
		if res.StatusCode != 200 {
			return &response{res, body}, nil
		}
		ttl := r.getTTL(req)
		if err := r.cache.Store(key, newCachedResponse(res, body), ttl); err != nil {
			return &response{res, body}, err
		}
		return &response{res, body}, nil
	})
	if err != nil {
		return nil, err
	}
	rr := v.(*response)
	res := rr.res
	if shared {
		// Create a copy of the response
		dup := *res
		res = &dup
	}
	res.Body = io.NopCloser(bytes.NewReader(rr.body))
	return res, nil
}

type cachedResponse struct {
	// WARN: we don't need to store this
	// Status           string      `json:"status"`
	StatusCode       int         `json:"status_code"`
	Proto            string      `json:"proto"`
	ProtoMajor       int         `json:"proto_major"`
	ProtoMinor       int         `json:"proto_minor"`
	Header           http.Header `json:"header,omitempty"` // WARN: careful including all headers
	Body             []byte      `json:"body,omitempty"`
	ContentLength    int64       `json:"content_length"`
	TransferEncoding []string    `json:"transfer_encoding,omitempty"`
	Close            bool        `json:"close"`
	// WARN: Uncompressed
	Uncompressed bool        `json:"uncompressed"`
	Trailer      http.Header `json:"trailer,omitempty"`
}

func newCachedResponse(res *http.Response, body []byte) *cachedResponse {
	return &cachedResponse{
		// Status:           res.Status,
		StatusCode:       res.StatusCode,
		Proto:            res.Proto,
		ProtoMajor:       res.ProtoMajor,
		ProtoMinor:       res.ProtoMinor,
		Header:           res.Header,
		Body:             body,
		ContentLength:    res.ContentLength,
		TransferEncoding: res.TransferEncoding,
		Close:            res.Close,
		Uncompressed:     res.Uncompressed,
		Trailer:          res.Trailer,
	}
}

func (r *cachedResponse) Response(req *http.Request) *http.Response {
	body := r.Body
	if isGzipd(body) {
		if gr, err := gzip.NewReader(bytes.NewReader(body)); err == nil {
			var r bytes.Buffer
			if _, err := r.ReadFrom(gr); err == nil {
				body = r.Bytes()
			}
			if err := gr.Close(); err != nil {
				// WARN
			}
			// WARN: need to close the gzip reader
		}
	}
	return &http.Response{
		Status:           http.StatusText(r.StatusCode),
		StatusCode:       r.StatusCode,
		Proto:            r.Proto,
		ProtoMajor:       r.ProtoMajor,
		ProtoMinor:       r.ProtoMinor,
		Header:           r.Header,
		Body:             io.NopCloser(bytes.NewReader(body)),
		ContentLength:    r.ContentLength,
		TransferEncoding: r.TransferEncoding,
		Close:            r.Close,
		Uncompressed:     r.Uncompressed,
		Trailer:          r.Trailer,
		Request:          req,
		TLS:              nil, // WARN: not set
	}
}
