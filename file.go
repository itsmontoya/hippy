package hippy

import (
	"io"
	"os"
	"path/filepath"
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
	f    *os.File
	path string
	name string

	hasMW bool
	mws   []Middleware
	mww   io.WriteCloser
	mwr   io.ReadCloser

	closed bool
}

func (f *file) setW() (err error) {
	if !f.hasMW {
		return
	}

	if f.mww != nil {
		return
	}

	if f.mww, err = newMWWriter(f.f, f.mws); err != nil {
		f.mww = nil
		return
	}

	return
}

func (f *file) setR() (err error) {
	if !f.hasMW {
		return
	}

	if f.mwr != nil {
		return
	}

	if f.mwr, err = newMWReader(f.f, f.mws); err != nil {
		f.mwr = nil
		return
	}

	return
}

func (f *file) Write(b []byte) (n int, err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	if !f.hasMW {
		return f.f.Write(b)
	}

	if err = f.setW(); err != nil {
		return
	}

	return f.mww.Write(b)
}

func (f *file) Read(b []byte) (n int, err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	if !f.hasMW {
		return f.f.Read(b)
	}

	if err = f.setR(); err != nil {
		return
	}

	return f.mwr.Read(b)
}

func (f *file) Flush() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	if f.hasMW {
		if f.mww != nil {
			f.mww.Close()
			f.mww = nil
		}

		if f.mwr != nil {
			f.mwr.Close()
			f.mwr = nil
		}
	}

	return f.f.Sync()
}

func (f *file) Close() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	if err = f.Flush(); err != nil {
		return
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
	if f.f, err = os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644); err != nil {
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

	var fi os.FileInfo
	if fi, err = f.f.Stat(); err != nil {
		return err
	}

	_, err = f.f.Seek(fi.Size(), 0)
	return
}
