package gossiper_service

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
	nonce_helper "github.com/nayotta/metathings/pkg/common/nonce"
	opt_helper "github.com/nayotta/metathings/pkg/common/option"
	"github.com/sirupsen/logrus"
)

type RedisStreamSourceOption struct {
	Addr     string
	Db       int
	Password string
	Topic    string
	Nonce    string
}

type RedisStreamSource struct {
	opt    *RedisStreamSourceOption
	client *redis.Client
	logger logrus.FieldLogger
}

func (s *RedisStreamSource) get_logger() logrus.FieldLogger {
	return s.logger.WithFields(logrus.Fields{
		"nonce": s.opt.Nonce,
	})
}

func (s *RedisStreamSource) Pull() (map[string]interface{}, error) {
	for {
		vals, err := s.client.XReadGroup(&redis.XReadGroupArgs{
			Group:    s.opt.Nonce,
			Consumer: s.opt.Nonce,
			Streams:  []string{s.opt.Topic, ">"},
			Count:    1,
			Block:    3 * time.Second,
			NoAck:    true,
		}).Result()

		switch err {
		case redis.Nil:
			continue
		case nil:
		default:
			s.get_logger().WithError(err).Debugf("failed to read redis stream")
			return nil, err
		}

		for _, val := range vals {
			for _, msg := range val.Messages {
				var ok bool
				var buf interface{}

				if buf, ok = msg.Values["data"]; ok {
					r := map[string]interface{}{}
					if err = json.Unmarshal([]byte(buf.(string)), &r); err != nil {
						s.get_logger().WithError(err).Debugf("failed to unmarshal json string")
						return nil, err
					}

					return r, nil
				}
			}
		}
	}
}

func (s *RedisStreamSource) Close() error {
	logger := s.get_logger().WithFields(logrus.Fields{"nonce": s.opt.Nonce})
	if err := s.client.XGroupDestroy(s.opt.Topic, s.opt.Nonce).Err(); err != nil {
		logger.WithError(err).Debugf("failed to destory redis stream group")
		return err
	}

	return nil
}

func NewRedisStreamSource(args ...interface{}) (Source, error) {
	var logger logrus.FieldLogger
	opt := new(RedisStreamSourceOption)
	opt.Nonce = nonce_helper.GenerateNonce()

	if err := opt_helper.Setopt(map[string]func(string, interface{}) error{
		"logger":   opt_helper.ToLogger(&logger),
		"addr":     opt_helper.ToString(&opt.Addr),
		"db":       opt_helper.ToInt(&opt.Db),
		"password": opt_helper.ToString(&opt.Password),
		"topic":    opt_helper.ToString(&opt.Topic),
	})(args...); err != nil {
		return nil, err
	}

	redis_client_options := &redis.Options{
		Addr: opt.Addr,
		DB:   opt.Db,
	}
	if opt.Password != "" {
		redis_client_options.Password = opt.Password
	}

	client := redis.NewClient(redis_client_options)
	if err := client.XGroupCreateMkStream(opt.Topic, opt.Nonce, "$").Err(); err != nil {
		return nil, err
	}

	src := &RedisStreamSource{
		opt:    opt,
		client: client,
		logger: logger,
	}

	return src, nil
}

func init() {
	register_source_factory("redis-stream", NewRedisStreamSource)
}
