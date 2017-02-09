package hippy

import (
	"github.com/go-ini/ini"
)

// defaultOptionsare the default options for Hippy.
// Note: CopyOn{Write,Read}'s values can technically be assumed. They are defined so that their value is explicit
var defaultOptions = Opts{
	CopyOnWrite: false,
	CopyOnRead:  false,

	ArchiveOnClose: true,
	CompactOnClose: true,

	AsyncBackend: false,
}

// NewOpts returns new options for Hippy
func NewOpts(src interface{}) (o Opts, err error) {
	if src == nil {
		o = defaultOptions
		return
	}

	if err = ini.MapTo(&o, src); err != nil {
		return
	}

	return
}

// Opts are options for Hippy
type Opts struct {
	CopyOnWrite bool `ini:"copyOnWrite"`
	CopyOnRead  bool `ini:"copyOnRead"`

	ArchiveOnClose bool `ini:"archiveOnClose"`
	CompactOnClose bool `ini:"compactOnClose"`

	AsyncBackend bool `ini:"asyncBackend"`
}
