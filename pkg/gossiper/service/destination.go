package gossiper_service

import "sync"

type Destination interface {
	Push(interface{}) error
	Close() error
}

type DestinationFactory func(...interface{}) (Destination, error)

var (
	destination_factories      map[string]DestinationFactory
	destination_factories_once sync.Once
)

func register_destination_factory(name string, fty DestinationFactory) {
	destination_factories_once.Do(func() {
		destination_factories = map[string]DestinationFactory{}
	})
	destination_factories[name] = fty
}

func NewDestination(name string, args ...interface{}) (Destination, error) {
	fty, ok := destination_factories[name]
	if !ok {
		return nil, ErrUnsupportedDestinationDriver
	}

	return fty(args...)
}
