package gossiper_service

import "sync"

type Source interface {
	Pull() (map[string]interface{}, error)
	Close() error
}

type SourceFactory func(...interface{}) (Source, error)

var (
	source_factories      map[string]SourceFactory
	source_factories_once sync.Once
)

func register_source_factory(name string, fty SourceFactory) {
	source_factories_once.Do(func() {
		source_factories = map[string]SourceFactory{}
	})
	source_factories[name] = fty
}

func NewSource(name string, args ...interface{}) (Source, error) {
	fty, ok := source_factories[name]
	if !ok {
		return nil, ErrUnsupportedSourceDriver
	}

	return fty(args...)
}
