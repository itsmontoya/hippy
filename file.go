package hippy

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"

	"fmt"
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

func (f *file) SeekToNextLine() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	var (
		buf    [32]byte // Buffer
		n      int
		nlf    bool
		offset int64 = -1
	)

	for n, err = f.f.Read(buf[:]); err == nil; n, err = f.f.Read(buf[:]) {
		for i, b := range buf[:n] {
			if b == _newline {
				nlf = true
			} else if nlf {
				if b > 0 {
					offset = int64(n - i)
				}

				break
			}
		}

		if offset > -1 {
			break
		}
	}

	if err != nil {
		return
	}

	if offset == -1 {
		return ErrLineNotFound
	}

	_, err = f.f.Seek(-offset, 1)
	return
}

func (f *file) SeekToPrevLine() (err error) {
	if f.closed {
		return ErrIsClosed
	}

	var (
		buf    [32]byte // Buffer
		n      int
		curr   int64
		nlc    int
		offset int64 = -1
	)

	peek(f.f)

	if curr, err = f.f.Seek(-32, 1); err != nil {
		if _, err = f.f.Seek(0, 0); err != nil {
			return
		}

		err = nil
	}

	for offset == -1 && err == nil {
		for n, err = f.f.Read(buf[:]); err == nil; n, err = f.f.Read(buf[:]) {
			fmt.Println("About to iterate through buf", buf[:n])
			reverseByteSlice(buf[:n])
			for i, b := range buf[:n] {
				if b == _newline {
					nlc++
				}

				if nlc == 2 || nlc == 1 && curr == 0 {
					offset = int64(i)
					fmt.Println("Setting offset", n, i, offset)
					break
				}
			}

			if offset > -1 {
				break
			}
		}

		if curr == 0 || offset > -1 {
			break
		}

		if curr, err = f.f.Seek(-64, 1); err != nil {
			if _, err = f.f.Seek(0, 0); err != nil {
				return
			}

			err = nil
		}
	}

	reverseByteSlice(buf[:n])
	fmt.Println("Offset", offset)
	if offset == -1 {
		return ErrLineNotFound
	}

	peek(f.f)
	_, err = f.f.Seek(-offset, 1)
	peek(f.f)
	return
}

func peek(f *os.File) {
	var pkk [32]byte
	n, _ := f.Read(pkk[:])
	f.Seek(int64(-n), 1)
	fmt.Println("Peeek??", pkk[:n])
}

func (f *file) WriteLine(b []byte) (err error) {
	if f.closed {
		err = ErrIsClosed
		return
	}

	// Write our prefix byte (action) without any middlewares so we can find a line action without decoding
	if err = f.buf.WriteByte(b[0]); err != nil {
		return
	}

	if f.hasMW {
		if b, err = writeMWBytes(b[1:], f.mws); err != nil {
			return
		}
	}

	if _, err = f.buf.Write(b); err != nil {
		return
	}

	// Write our suffix byte (newline) without any middlewares so we can find a line-end without decoding
	return f.buf.WriteByte(_newline)
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

func (fr *fileReader) readLine() (a byte, key string, b []byte, err error) {
	b = fr.scnr.Bytes()
	a = b[0]
	b = b[1:]

	if !fr.f.hasMW {
		goto END
	}

	if b, err = readMWBytes(b, fr.f.mws); err != nil {
		return
	}

END:
	key, b, err = parseLogLine(a, b)
	return
}

func (fr *fileReader) ReadLines(fn func(byte, string, []byte, error) error) (err error) {
	for fr.scnr.Scan() {
		if err = fn(fr.readLine()); err != nil {
			return
		}
	}

	return
}
