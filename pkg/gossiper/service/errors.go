package gossiper_service

import "errors"

var (
	ErrUnsupportedSourceDriver      = errors.New("unsupported source driver")
	ErrUnsupportedDestinationDriver = errors.New("unsupported destination driver")
)
