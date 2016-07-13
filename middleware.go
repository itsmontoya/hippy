package hippy

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"io/ioutil"

	"github.com/missionMeteora/toolkit/crypty"
	"github.com/missionMeteora/toolkit/errors"
)

// Middleware is the interface that defines an encoder/decoder chain.
type Middleware interface {
	Name() string
	Writer(w io.Writer) (io.WriteCloser, error)
	Reader(r io.Reader) (io.ReadCloser, error)
}

type readClosers []io.ReadCloser

func (rcs readClosers) Close() error {
	var errs ErrorList
	for _, rc := range rcs {
		if rc == nil {
			continue
		}

		errs = errs.Append(rc.Close())
	}

	return errs.Err()
}

type writeClosers []io.WriteCloser

func (wcs writeClosers) Close() error {
	var errs ErrorList
	for _, wc := range wcs {
		if wc == nil {
			continue
		}

		errs = errs.Append(wc.Close())
	}

	return errs.Err()
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

// NewCryptyMW returns a new Crypty middleware
func NewCryptyMW(key, iv []byte) *CryptyMW {
	return &CryptyMW{key, iv}
}

// CryptyMW handles encryption
type CryptyMW struct {
	key []byte
	iv  []byte
}

// Name returns the middleware name
func (c *CryptyMW) Name() string {
	return "encryption/crypty"
}

// Writer returns a new gzip writer
func (c *CryptyMW) Writer(w io.Writer) (io.WriteCloser, error) {
	return crypty.NewWriterPair(w, c.key, c.iv)
}

// Reader returns a new gzip reader
func (c *CryptyMW) Reader(r io.Reader) (rc io.ReadCloser, err error) {
	return crypty.NewReaderPair(r, c.key, c.iv)
}

type b64MW struct {
}

// Name returns the middleware name
func (b b64MW) Name() string {
	return "encoding/base64"
}

// Writer returns a new gzip writer
func (b b64MW) Writer(w io.Writer) (io.WriteCloser, error) {
	return base64.NewEncoder(base64.StdEncoding, w), nil
}

// Reader returns a new gzip reader
func (b b64MW) Reader(r io.Reader) (rc io.ReadCloser, err error) {
	return ioutil.NopCloser(base64.NewDecoder(base64.StdEncoding, r)), nil
}

func readMWBytes(in []byte, mws []Middleware) (out []byte, err error) {
	var (
		rcs  readClosers
		oBuf = bytes.NewBuffer(nil)
	)

	if rcs, err = getReadClosers(in, mws); err != nil {
		return
	}

	io.Copy(oBuf, rcs[0])

	if err = rcs.Close(); err != nil {
		return
	}

	out = oBuf.Bytes()
	return
}

type MWs struct {
	s []Middleware
}

func (m *MWs) Writer()

func (m *MWs) Reader

func writeMWBytes(w io.Writer, b []byte, mws []Middleware) (err error) {
	var wcs writeClosers
	if wcs, err = getWriteClosers(w, mws); err != nil {
		return
	}

	var errs errors.ErrorList
	_, err = wcs[0].Write(b)
	errs = errs.Append(err)

	err = wcs.Close()
	errs = errs.Append(err)

	return errs.Err()
}

func getReadClosers(in []byte, mws []Middleware) (rcs readClosers, err error) {
	var (
		rdr io.ReadCloser
		mwl = len(mws)
	)

	rcs = make(readClosers, mwl)
	for i, mw := range mws {
		if i == 0 {
			if rdr, err = mw.Reader(bytes.NewBuffer(in)); err != nil {
				goto END
			}
		} else {
			if rdr, err = mw.Reader(rdr); err != nil {
				goto END
			}
		}

		rcs[mwl-1-i] = rdr
	}

END:
	if err != nil {
		rcs.Close()
	}

	return
}

func getWriteClosers(in io.Writer, mws []Middleware) (wcs writeClosers, err error) {
	var (
		wtr io.WriteCloser
		mwl = len(mws)
	)

	wcs = make(writeClosers, mwl)
	for i, mw := range mws {
		if i == 0 {
			if wtr, err = mw.Writer(in); err != nil {
				goto END
			}
		} else {
			if wtr, err = mw.Writer(wtr); err != nil {
				goto END
			}
		}

		wcs[mwl-1-i] = wtr
	}

END:
	if err != nil {
		wcs.Close()
	}

	return
}
