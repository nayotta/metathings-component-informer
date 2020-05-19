package gossiper_service

import (
	cfg_helper "github.com/nayotta/metathings/pkg/common/config"
	component "github.com/nayotta/metathings/pkg/component"
	"github.com/spf13/cast"
	"github.com/stretchr/objx"
)

/*
 * Kernel Configs:
 *   Fields:
 *     gossipers:
 *       Type: Array<Gossiper>
 *   Examples:
 *     ...
 *     gossipers:
 *     - sources:
 *       - driver: redis-stream
 *         addr: 127.0.0.1
 *         db: 0
 *         topic: test
 *       destinations:
 *       - driver: flow
 *         flow_name: greeting
 *     ...
 *
 * Gossiper:
 *   Fields:
 *     name:
 *       Type: string
 *     sources:
 *       Type: Array<Source>
 *     destinations:
 *       Type: Array<Destination>
 *
 * Source:
 *   Fields:
 *     driver:
 *       Type: string
 *     ...
 *
 * Destination:
 *   Fields:
 *     driver:
 *       Type: string
 *     ...
 *
 * RedisStreamSource:
 *   Fields:
 *     driver:
 *       Type: string
 *       Const: redis-stream
 *     addr:
 *       Type: string
 *     db:
 *       Type: number
 *     password:
 *       Type: string
 *     topic:
 *       Type: string
 *
 * FlowDestination:
 *   Fields:
 *     driver:
 *       Type: string
 *       Const: flow
 *     flow_name:
 *       Type: string
 */

type GossiperService struct {
	module           *component.Module
	err              error
	running          bool
	assert_callbacks []func() error
}

func (s *GossiperService) startup() {
	s.running = true
	s.err = nil

	kc := s.module.Kernel().Config()
	for _, gossiper := range cast.ToSlice(kc.Get("gossipers")) {
		go s.gossiper_loop(gossiper)
	}
}

func (s *GossiperService) assert(err error) {
	if err != nil {
		s.err = err
		s.running = false
		s.module.Logger().WithError(err).Errorf("assert error")
		for _, cb := range s.assert_callbacks {
			defer cb()
		}
		defer s.module.Stop()
	}
}

func (s *GossiperService) is_running() bool {
	return s.running
}

func (s *GossiperService) gossiper_loop(gossiper interface{}) {
	var err error
	var srcs []Source
	var dsts []Destination
	cfg := objx.New(cast.ToStringMap(gossiper))

	cfg.Get("sources").EachInter(func(i int, v interface{}) bool {
		var drv string
		var args []interface{}
		var src Source

		drv, args, err = cfg_helper.ParseConfigOption("driver", cast.ToStringMap(v),
			"logger", s.module.Logger(),
		)
		s.assert(err)

		src, err = NewSource(drv, args...)
		s.assert(err)
		s.assert_callbacks = append(s.assert_callbacks, src.Close)

		srcs = append(srcs, src)

		return true
	})

	cfg.Get("destinations").EachInter(func(i int, v interface{}) bool {
		var drv string
		var args []interface{}
		var dst Destination

		drv, args, err = cfg_helper.ParseConfigOption("driver", cast.ToStringMap(v),
			"logger", s.module.Logger(),
		)
		s.assert(err)

		switch drv {
		case "flow":
			args = append(args, "kernel", s.module.Kernel())
		}

		dst, err = NewDestination(drv, args...)
		s.assert(err)
		s.assert_callbacks = append(s.assert_callbacks, dst.Close)

		dsts = append(dsts, dst)

		return true
	})

	pipe := make(chan map[string]interface{})
	for _, src := range srcs {
		go func() {
			for s.is_running() {
				dat, err := src.Pull()
				s.assert(err)
				pipe <- dat
			}
		}()
	}

	go func() {
		for s.is_running() {
			dat := <-pipe
			for _, dst := range dsts {
				err = dst.Push(dat)
				s.assert(err)
			}
		}
	}()
}

func (s *GossiperService) InitModuleService(m *component.Module) error {
	s.module = m
	go s.startup()

	return nil
}
