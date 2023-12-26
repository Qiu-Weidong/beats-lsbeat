package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/Qiu-Weidong/lsbeat/config"
)

// lsbeat configuration.
type lsbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client

	lastIndexTime time.Time
}

// New creates an instance of lsbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	// fmt.Printf("config period = %d", c.Period)
	// logp.Info("config period = %f, config path = %s", c.Period.Seconds(), c.Path)

	bt := &lsbeat{
		done:   make(chan struct{}),
		config: c,

		// 初始化 lastIndexTime
		lastIndexTime: time.Now(),
	}
	return bt, nil
}

// Run starts lsbeat.
func (bt *lsbeat) Run(b *beat.Beat) error {
	logp.Info("lsbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	cnt := 1

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		// 在这里写代码逻辑
		logp.Info("before sleep cnt = %d", cnt)
		// 休眠 10s 钟
		time.Sleep(10 * time.Second)
		logp.Info("after sleep cnt = %d", cnt)
		cnt += 1

		logp.Info("Event sent")
	}
}

// Stop stops lsbeat.
func (bt *lsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *lsbeat) collectJobs(baseDir string, b *beat.Beat) {
	now := time.Now()
	// 更新时间
	bt.lastIndexTime = now

	event := beat.Event{
		Timestamp: now,
		Fields: common.MapStr{
			"type": "job",
		},
	}
	bt.client.Publish(event)
}
