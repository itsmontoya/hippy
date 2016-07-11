package hippy

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func newFile(path, name string, mws []Middleware, set bool) (f *file, err error) {
	f = &file{
		path: path,
		name: name,

		hasMW: len(mws) > 0,
		mws:   mws,

		closed: true,
	}

	if !set {
		return
	}

	if err = f.SetFile(); err != nil {
		f = nil
	}
	return
}

type file struct {
	mux sync.Mutex

	f    *os.File
	buf  *bufio.Writer
	path string
	name string

	hasMW bool
	mws   []Middleware

	closed bool
}

func (f *file) Close() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	if err = f.buf.Flush(); err != nil {
		return
	}

	if err = f.f.Close(); err != nil {
		return
	}

	f.f = nil
	f.buf = nil
	f.closed = true
	return
}

func (f *file) SetFile() (err error) {
	if !f.closed {
		return ErrIsOpen
	}

	fn := filepath.Join(f.path, f.name+".hdb")
	// Open persistance file
	if f.f, err = os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}

	f.buf = bufio.NewWriter(f.f)
	f.closed = false
	return
}

func (f *file) SeekToStart() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	_, err = f.f.Seek(0, 0)
	return
}

func (f *file) SeekToEnd() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	_, err = f.f.Seek(0, os.SEEK_END)
	return
}

func (f *file) SeekToLastHash() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	if err = f.SeekToStart(); err != nil {
		return
	}

	var (
		buf [32]byte // Buffer
		ttl int64
		n   int

		hs  int64 = -1   // Hash start
		inl       = true // Is new line
	)

	for n, err = f.f.Read(buf[:]); err == nil; n, err = f.f.Read(buf[:]) {
		for i, b := range buf[:n] {
			if b == _newline {
				inl = true
				continue
			}

			if !inl {
				continue
			}

			if b == _hash {
				hs = ttl + int64(i)
			}

			inl = false
		}

		ttl += int64(n)
	}

	if err == io.EOF {
		err = nil
	} else if err != nil {
		return
	}

	if hs == -1 {
		return ErrHashNotFound
	}

	_, err = f.f.Seek(hs, 0)
	return
}

func (f *file) WriteLine(b []byte) (err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	if f.hasMW {
		if b, err = writeMWBytes(b, f.mws); err != nil {
			return
		}
	}

	b = append(b, _newline)
	_, err = f.buf.Write(b)
	return
}

func (f *file) Flush() (err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	if err = f.buf.Flush(); err != nil {
		return
	}

	return f.f.Sync()
}

func (f *file) Read(fn func(r *fileReader) error) (err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	fr := fileReader{
		f:    f,
		scnr: bufio.NewScanner(f.f),
	}

	err = fn(&fr)

	fr.f = nil
	fr.scnr = nil
	return
}

type fileReader struct {
	f    *file
	scnr *bufio.Scanner
}

func (fr *fileReader) ReadLine() (b []byte, err error) {
	if !fr.scnr.Scan() {
		err = io.EOF
		return
	}

	if !fr.f.hasMW {
		b = fr.scnr.Bytes()
		return
	}

	return readMWBytes(fr.scnr.Bytes(), fr.f.mws)
}
