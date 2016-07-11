package hippy

import (
	"bytes"
	"sync"
)

var bp = newBufferPool(32)

func newBufferPool(sz int) *bufferPool {
	return &bufferPool{
		p: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, sz))
			},
		},
	}
}

type bufferPool struct {
	p sync.Pool
}

func (p *bufferPool) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}

func (p *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.p.Put(buf)
}
