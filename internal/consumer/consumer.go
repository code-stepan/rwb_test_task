package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/wildberries/trending-search-service/internal/config"
	"github.com/wildberries/trending-search-service/internal/models"
)

type Consumer struct {
	client  *kgo.Client
	handler func(*models.SearchEvent) bool
	ready   chan struct{}
}

func New(cfg *config.Config, handler func(*models.SearchEvent) bool) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers),
		kgo.ConsumerGroup(cfg.KafkaGroupID),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.FetchMaxBytes(50_000_000),
		kgo.FetchMinBytes(1_000_000),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.AutoCommitInterval(5 * time.Second),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return &Consumer{
		client:  client,
		handler: handler,
		ready:   make(chan struct{}),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	close(c.ready)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := c.client.PollRecords(ctx, 1000)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("kafka error: topic=%s partition=%d err=%v", topic, partition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			var evt models.SearchEvent
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				log.Printf("unmarshal error: %v (value=%s)", err, string(r.Value))
				return
			}
			c.handler(&evt)
		})
	}
}

func (c *Consumer) Ready() <-chan struct{} {
	return c.ready
}

func (c *Consumer) Close() {
	c.client.Close()
}
