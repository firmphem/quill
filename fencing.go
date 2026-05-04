package main

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli        *clientv3.Client
	leaseID    clientv3.LeaseID
	isLeader   bool
	instanceID = hostname()
)

// -----------------------------------------------------------------------------
func hostname() string {
	h, _ := os.Hostname()
	return h
}

// -----------------------------------------------------------------------------
func initEtcd(endpoints []string) error {
	var err error
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	return err
}

// -----------------------------------------------------------------------------
func amILeader() bool {
	return isLeader
}

// -----------------------------------------------------------------------------
func waitUntilLeader(ctx context.Context, cancel context.CancelFunc) error {
	retryInterval := time.Second
	isInstanceLeader.WithLabelValues().Set(0)

	logTicker := time.NewTicker(time.Minute)
	defer logTicker.Stop()

	log.Info().Msg("starting leader election loop...")

	for {
		select {
		case <-logTicker.C:
			log.Info().Msg("checking if can become a leader (background check)...")
		default:
		}

		ok, err := tryBecomeLeader(ctx, cancel)
		if err != nil {
			log.Error().Err(err).Msg("leader election error")
		}

		if ok {
			return nil
		}

		select {
		case <-time.After(retryInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// -----------------------------------------------------------------------------
func tryBecomeLeader(ctx context.Context, cancel context.CancelFunc) (bool, error) {
	cfg := currentConfig.Load().(*Config)

	rpcCtx, rpcCancel := context.WithTimeout(ctx, 3*time.Second)
	defer rpcCancel()

	leaseResp, err := cli.Grant(rpcCtx, int64(cfg.Etcd.LeaseTTLSeconds))
	if err != nil {
		return false, err
	}

	txn := cli.Txn(rpcCtx).
		If(clientv3.Compare(clientv3.CreateRevision(cfg.Etcd.EtcdLeaderKey), "=", 0)).
		Then(clientv3.OpPut(cfg.Etcd.EtcdLeaderKey, instanceID, clientv3.WithLease(leaseResp.ID)))

	resp, err := txn.Commit()
	if err != nil {
		return false, err
	}

	if !resp.Succeeded {
		return false, nil
	}

	// we are about to become a leader
	becomeLeader(ctx, leaseResp.ID, cancel)
	return true, nil
}

// -----------------------------------------------------------------------------
func becomeLeader(ctx context.Context, id clientv3.LeaseID, cancel context.CancelFunc) {
	isLeader = true
	leaseID = id

	log.Info().Str("instanceID", instanceID).Msg("became a leader")
	isInstanceLeader.WithLabelValues().Set(1)

	go keepAliveLoop(ctx, id, cancel)
}

// -----------------------------------------------------------------------------
func keepAliveLoop(ctx context.Context, id clientv3.LeaseID, cancel context.CancelFunc) {
	ch, err := cli.KeepAlive(ctx, id)
	if err != nil {
		log.Error().Err(err).Msg("keepalive failed:")
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				log.Warn().Msg("Lost etcd keepalive...")
				stopLeadership()
				cancel()
				return
			}
		}
		// _, ok := <-ch
		// if !ok {
		// 	log.Warn().Msg("Lost etcd keepalive. Triggering graceful shutdown to avoid split brain...")
		// 	stopLeadership()
		// 	cancel()
		// 	return
		// }
	}
}

// -----------------------------------------------------------------------------
func stopLeadership() {
	if leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		log.Info().Msg("attempting to revoke etcd lease...")
		_, err := cli.Revoke(ctx, leaseID)
		if err != nil {
			log.Warn().Err(err).Msg("could not revoke lease (etcd might be unreachable)")
		}
	}

	isInstanceLeader.WithLabelValues().Set(0)
	isLeader = false
	log.Info().Msg("leadership state cleared locally")
}

// -----------------------------------------------------------------------------
func waitForLeadership(ctx context.Context, cancel context.CancelFunc) {
	cfg := currentConfig.Load().(*Config)
	err := initEtcd(cfg.Etcd.Endpoints)
	if err != nil {
		panic(err)
	}

	log.Info().Msg("waiting to become leader")
	err = waitUntilLeader(ctx, cancel)
	if err != nil {
		log.Error().Err(err).Msg("cannot become a leader")
		cancel()
		return
	}
	log.Info().Msg("leadership was acquired")
}
