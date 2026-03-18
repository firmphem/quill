package main

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

var commitsDoneAtomic atomic.Int32

const (
	KafkaOffsetSuccessful = "successful"
	KafkaOffsetFailed     = "failed"
)

// -----------------------------------------------------------------------------
func incKafkaOffsetCommitMetric(err error) {
	status := KafkaOffsetSuccessful
	if err != nil {
		status = KafkaOffsetFailed
	}
	kafkaOffsetCommitTotal.WithLabelValues(status).Inc()
}

// -----------------------------------------------------------------------------
func commitManager(ctx context.Context, partitionsMu *sync.Mutex, partitions map[int32]*partitionState, topic string, consumer *kafka.Consumer, ackCh <-chan Ack, commitDone chan struct{}) {
	cfg := currentConfig.Load().(*Config)

	commitIntervalMs := cfg.Quill.KafkaOffsetCommit.EveryMilliseconds
	commitBatchMessages := cfg.Quill.KafkaOffsetCommit.EveryMessages
	commitTicker := time.NewTicker(time.Duration(commitIntervalMs) * time.Millisecond)
	defer commitTicker.Stop()

	var commitWG sync.WaitGroup
	commitWG.Add(1)
	defer commitWG.Done()
	for {
		select {
		case <-ctx.Done():
			// final commit for all partitions we still have and if something was not yet commited due to batching
			partitionsMu.Lock()
			var offsets []kafka.TopicPartition
			for p, ps := range partitions {
				if ps != nil && ps.lastProcessed >= 0 && ps.lastProcessed != ps.lastCommitted {
					offsets = append(offsets, kafka.TopicPartition{
						Topic:     &topic,
						Partition: p,
						Offset:    ps.lastProcessed + 1,
					})
				}
			}
			partitionsMu.Unlock()

			if len(offsets) > 0 {
				log.Debug().Interface("offsets", offsets).Msg("final commit on shutdown. we won't retry it.")
				if _, err := consumer.CommitOffsets(offsets); err != nil {
					incKafkaOffsetCommitMetric(err)
					log.Error().Err(err).Msg("final commit failed")
				} else {
					incKafkaOffsetCommitMetric(nil)
					partitionsMu.Lock()
					for _, tp := range offsets {
						if ps, ok := partitions[tp.Partition]; ok {
							ps.lastCommitted = tp.Offset - 1
							ps.msgCount = 0
						}
					}
					partitionsMu.Unlock()
				}
			}
			close(commitDone)
			return

		case ack := <-ackCh:
			s := tracker.startStage("commit")
			// update partition state
			partitionsMu.Lock()
			ps, ok := partitions[ack.Partition]
			if !ok {
				ps = &partitionState{lastProcessed: -1, lastCommitted: -1, msgCount: 0}
				partitions[ack.Partition] = ps
			}
			if ack.Offset > ps.lastProcessed {
				ps.lastProcessed = ack.Offset
			}
			ps.msgCount++

			// immediate commit requested by worker (only for test reason here)
			if ack.CommitNow {
				offsets := []kafka.TopicPartition{{
					Topic:     &topic,
					Partition: ack.Partition,
					Offset:    ps.lastProcessed + 1,
				}}
				if _, err := consumer.CommitOffsets(offsets); err != nil {
					incKafkaOffsetCommitMetric(err)
					log.Error().Int32("partition", ack.Partition).Err(err).Msg("immediate commit failed")
				} else {
					incKafkaOffsetCommitMetric(nil)
					ps.lastCommitted = ps.lastProcessed
					ps.msgCount = 0
					log.Debug().Int32("partition", ack.Partition).Int64("offset", int64(ps.lastCommitted+1)).Msg("immediate commit done")
				}
			}
			partitionsMu.Unlock()

			// commit by count threshold
			if commitBatchMessages > 0 {
				partitionsMu.Lock()
				if ps, ok := partitions[ack.Partition]; ok && ps.msgCount >= commitBatchMessages {
					offsets := []kafka.TopicPartition{{
						Topic:     &topic,
						Partition: ack.Partition,
						Offset:    ps.lastProcessed + 1,
					}}

					err := retryUntilDone(
						ctx,
						"commitByBatch",
						func(ctx context.Context) error {
							_, err := consumer.CommitOffsets(offsets)
							incKafkaOffsetCommitMetric(err)
							return err
						},
						isRetriableErr,
						"failure",
						func(finalErr error) error {
							return finalErr
						},
					)

					// if _, err := consumer.CommitOffsets(offsets); err != nil {
					if err != nil {
						if !isRetriableErr(err) {
							log.Error().Int32("partition", ack.Partition).Err(err).Msg("batch commit failed with not retriable error")
						}
					} else {
						commitsDoneAtomic.Add(int32(ps.msgCount))

						if exitAfterNMessages > 0 {
							current := commitsDoneAtomic.Load()
							if current >= exitAfterNMessages {
								log.Warn().Msg("hard quit right after commit. not a graceful stop at all")
								os.Exit(10)
							}
						}

						ps.lastCommitted = ps.lastProcessed
						ps.msgCount = 0
						log.Debug().Int32("partition", ack.Partition).Int64("offset", int64(ps.lastCommitted+1)).Msg("batch was committed")
					}
				}
				partitionsMu.Unlock()
			}
			s.end()

			// time-based commit for all partitions
		case <-commitTicker.C:
			s := tracker.startStage("commit")
			partitionsMu.Lock()
			var offsets []kafka.TopicPartition
			for p, ps := range partitions {
				if ps != nil && ps.lastProcessed >= 0 && ps.lastProcessed != ps.lastCommitted {
					offsets = append(offsets, kafka.TopicPartition{
						Topic:     &topic,
						Partition: p,
						Offset:    ps.lastProcessed + 1,
					})
				}
			}
			partitionsMu.Unlock()

			if len(offsets) > 0 {
				err := retryUntilDone(
					ctx,
					"commitByBatch",
					func(ctx context.Context) error {
						_, err := consumer.CommitOffsets(offsets)
						incKafkaOffsetCommitMetric(err)
						return err
					},
					isRetriableErr,
					"failure",
					func(finalErr error) error {
						return finalErr
					},
				)
				if err != nil {
					// if _, err := consumer.CommitOffsets(offsets); err != nil {
					if !isRetriableErr(err) {
						log.Error().Err(err).Msg("time-based commit failed with not retriable error")
					}
				} else {
					partitionsMu.Lock()
					for _, tp := range offsets {
						if ps, ok := partitions[tp.Partition]; ok {
							ps.lastCommitted = tp.Offset - 1
							ps.msgCount = 0
						}
					}
					partitionsMu.Unlock()
					log.Debug().Interface("offsets", offsets).Msg("time-based commit committed offsets")
				}
			}
			s.end()
		}
	}
}
