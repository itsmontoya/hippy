package hippy

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func newFile(path, name string, mws []Middleware) (f *file, err error) {
	f = &file{
		path: path,
		name: name,

		hasMW: len(mws) > 0,
		mws:   mws,

		closed: true,
	}

	if err = f.SetFile(); err != nil {
		f = nil
	}

	return
}

type file struct {
	mux sync.Mutex

	f    *os.File
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

	if err = f.f.Close(); err != nil {
		return
	}

	f.f = nil
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
	if _, err = f.f.Write(b); err != nil {
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
