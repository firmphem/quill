package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// -----------------------------------------------------------------------------
func fnRecreateKafkaTopic() (any, error) {
	if len(cfg.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided in config")
	}

	brokers := cfg.Kafka.Brokers[0]
	topic := cfg.Kafka.Topic

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	_, err = admin.DeleteTopics(ctx, []string{topic}, kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return nil, fmt.Errorf("failed to delete kafka topic: %w", err)
	}

	// 12 partitions, the rest params we do not care
	spec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     12,
		ReplicationFactor: 1,
		Config: map[string]string{
			"max.message.bytes": "400000000",
		},
	}

	time.Sleep(3 * time.Second)
	// for {
	// 	desc, err := admin.DescribeTopics(ctx, kafka.NewTopicCollectionOfTopicNames([]string{topic}))
	// 	if err != nil {
	// 		return nil, fmt.Errorf("DescribeTopics failed: %w", err)
	// 	}
	// 	if len(desc.TopicDescriptions) == 0 {
	// 		return nil, fmt.Errorf("unexpected: DescribeTopics returned no items")
	// 	}
	// 	if desc.TopicDescriptions[0].Error.Code() == kafka.ErrUnknownTopicOrPart {
	// 		break
	// 	}
	//
	// 	time.Sleep(300 * time.Millisecond)
	// }

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{spec}, kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return nil, fmt.Errorf("failed to post request to create kafka topic: %w", err)
	}

	for _, r := range results {
		if r.Topic == topic && r.Error.Code() != kafka.ErrNoError {
			return nil, fmt.Errorf("error creating topic %s: %v", topic, r.Error)
		}
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
func fnGetKafkaGroupConsumerCurrentOffset(partition int) (int, error) {
	info, err := getKafkaGroupConsumerInfo()
	if err != nil {
		return -1, err
	}

	for _, v := range info {
		if (v.Topic == cfg.Kafka.Topic) && (v.Partition == partition) {
			return int(v.Offset), nil
		}
	}

	return -1, fmt.Errorf("did not matching topic/partition")
}

// -----------------------------------------------------------------------------
func getKafkaGroupConsumerInfo() ([]KafkaCurrentOffsetInfo, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.Kafka.Brokers, ","),
		"group.id":           cfg.Kafka.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 6000,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer create: %w", err)
	}
	defer consumer.Close()

	md, err := consumer.GetMetadata(&cfg.Kafka.Topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}

	topicMD, ok := md.Topics[cfg.Kafka.Topic]
	if !ok {
		return nil, fmt.Errorf("topic not found in metadata: %s", cfg.Kafka.Topic)
	}

	if topicMD.Error.Code() != kafka.ErrNoError {
		return nil, fmt.Errorf("topic metadata error: %v", topicMD.Error)
	}

	partitions := make([]kafka.TopicPartition, 0, len(topicMD.Partitions))
	for _, p := range topicMD.Partitions {
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &cfg.Kafka.Topic,
			Partition: p.ID,
			Offset:    kafka.OffsetBeginning,
		})
	}

	if err := consumer.Assign(partitions); err != nil {
		return nil, fmt.Errorf("assign: %w", err)
	}

	committed, err := consumer.Committed(partitions, 5000)
	if err != nil {
		return nil, fmt.Errorf("committed: %w", err)
	}

	res := make([]KafkaCurrentOffsetInfo, 0, len(committed))
	for _, tp := range committed {
		off := int64(tp.Offset)
		if tp.Offset == kafka.OffsetInvalid {
			off = -1
		}

		res = append(res, KafkaCurrentOffsetInfo{
			Topic:     *tp.Topic,
			Partition: int(tp.Partition),
			Offset:    off,
		})
	}

	return res, nil
}
