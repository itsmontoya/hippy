package hippy

import (
	"compress/gzip"
	"io"
)

// Middleware is the interface that defines an encoder/decoder chain.
type Middleware interface {
	Name() string
	Writer(w io.Writer) (io.WriteCloser, error)
	Reader(r io.Reader) (io.ReadCloser, error)
}

func newMWWriter(in io.Writer, mws []Middleware) (out io.WriteCloser, err error) {
	mwl := len(mws)
	w := mwWriter{
		wcs: make([]io.WriteCloser, mwl),
		li:  mwl - 1,
	}

	for i, mw := range mws {
		curr := w.li - i
		if i == 0 {
			w.wcs[curr], err = mw.Writer(in)
		} else {
			w.wcs[curr], err = mw.Writer(w.wcs[curr+1])
		}
	}

	out = &w
	return
}

type mwWriter struct {
	wcs []io.WriteCloser
	li  int // Last index

	closed bool
}

func (w *mwWriter) Write(b []byte) (n int, err error) {
	return w.wcs[w.li].Write(b)
}

func (w *mwWriter) Close() (err error) {
	if w.closed {
		return ErrIsClosed
	}

	w.closed = true

	for _, wc := range w.wcs {
		wc.Close()
	}

	return
}

func newMWReader(in io.Reader, mws []Middleware) (out io.ReadCloser, err error) {
	mwl := len(mws)
	r := mwReader{
		rcs: make([]io.ReadCloser, mwl),
		li:  mwl - 1,
	}

	for i, mw := range mws {
		curr := r.li - i
		if i == 0 {
			r.rcs[curr], err = mw.Reader(in)
		} else {
			r.rcs[curr], err = mw.Reader(r.rcs[curr+1])
		}
	}

	out = &r
	return
}

type mwReader struct {
	rcs []io.ReadCloser
	li  int // Last index

	closed bool
}

func (r *mwReader) Read(b []byte) (n int, err error) {
	return r.rcs[r.li].Read(b)
}

func (r *mwReader) Close() (err error) {
	if r.closed {
		return ErrIsClosed
	}

	r.closed = true

	for _, rc := range r.rcs {
		rc.Close()
	}

	return
}

// GZipMW handles gzipping
type GZipMW struct {
}

// Name returns the middleware name
func (g GZipMW) Name() string {
	return "compress/gzip"
}

// Writer returns a new gzip writer
func (g GZipMW) Writer(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

// Reader returns a new gzip reader
func (g GZipMW) Reader(r io.Reader) (rc io.ReadCloser, err error) {
	if rc, err = gzip.NewReader(r); err != nil {
		rc = nil
	}

	return
}
