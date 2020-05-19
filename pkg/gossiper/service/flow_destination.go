package gossiper_service

import (
	opt_helper "github.com/nayotta/metathings/pkg/common/option"
	component "github.com/nayotta/metathings/pkg/component"
	"github.com/sirupsen/logrus"
)

type FlowDestinationOption struct {
	FlowName string
}

type FlowDestination struct {
	opt    *FlowDestinationOption
	stream *component.FrameStream
	logger logrus.FieldLogger
}

func (d *FlowDestination) Push(data interface{}) error {
	return d.stream.Push(data)
}

func (d *FlowDestination) Close() error {
	return d.stream.Close()
}

func NewFlowDestination(args ...interface{}) (Destination, error) {
	var opt FlowDestinationOption
	var knl *component.Kernel
	var logger logrus.FieldLogger

	if err := opt_helper.Setopt(map[string]func(string, interface{}) error{
		"flow_name": opt_helper.ToString(&opt.FlowName),
		"kernel": func(k string, v interface{}) error {
			var ok bool
			knl, ok = v.(*component.Kernel)
			if !ok {
				return opt_helper.InvalidArgument(k)
			}
			return nil
		},
		"logger": opt_helper.ToLogger(&logger),
	})(args...); err != nil {
		return nil, err
	}

	stm, err := knl.NewFrameStream(opt.FlowName)
	if err != nil {
		return nil, err
	}

	dst := &FlowDestination{
		opt:    &opt,
		stream: stm,
		logger: logger,
	}

	return dst, nil
}

func init() {
	register_destination_factory("flow", NewFlowDestination)
}
