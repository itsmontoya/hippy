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

// NewDefaultOpts will return simple default options
func NewDefaultOpts(path, name string) (o Opts) {
	o = defaultOptions
	o.Path = path
	o.Name = name

	o.ArchiveOnClose = false
	o.CompactOnClose = false
	return
}

// Opts are options for Hippy
type Opts struct {
	Path string `ini:"path"`
	Name string `ini:"name"`

	CopyOnWrite bool `ini:"copyOnWrite"`
	CopyOnRead  bool `ini:"copyOnRead"`

	ArchiveOnClose bool `ini:"archiveOnClose"`
	CompactOnClose bool `ini:"compactOnClose"`
}
