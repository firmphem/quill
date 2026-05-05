package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type TryFunc func(ctx context.Context) error
type IsRetriableFunc func(error) bool

// -----------------------------------------------------------------------------
func isRetriableErr(err error) bool {
	if err == nil {
		return true
	}

	// if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
	// 	return false
	// }

	cfg := currentConfig.Load().(*Config)

	if isErrorMatched(err, cfg.CausingFailureErrors) {
		log.Error().Str("got error", err.Error()).Msg("this error considered as fatal. will cause an abnormal quit")
		os.Exit(1)
	}

	isNonRetriableError := isErrorMatched(err, cfg.NonRetriableErrors)

	return !isNonRetriableError
}

// -----------------------------------------------------------------------------
func isErrorMatched(err error, list []ErrorMatcher) bool {
	errStr := strings.ToLower(err.Error())

	for _, matcher := range list {
		switch matcher.Matched {
		case ErrorMatchEquals:
			if errStr == strings.ToLower(matcher.Value) {
				return true
			}
		case ErrorMatchContains:
			if strings.Contains(errStr, strings.ToLower(matcher.Value)) {
				return true
			}
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// retry and error is not retriable fallback to another method if any OR fail
func retryUntilDone(ctx context.Context,
	tryFuncLabel string,
	tryFunc TryFunc,
	isRetriable IsRetriableFunc,
	onNonRetriableLabel string,
	onNonRetriable func(error) error,
) error {

	backoff := time.Second
	retryCount := 0

	for {
		err := func() (errResult error) {
			defer func() {
				if r := recover(); r != nil {
					errResult = fmt.Errorf("pgx panic recovered: %v", r)
					log.Error().Err(errResult).Msg("recovered from panic inside loop")
				}
			}()
			return tryFunc(ctx)
		}()

		if err == nil {
			if retryCount > 0 {
				log.Info().Str("op", tryFuncLabel).Int("retries", retryCount).Msg("operation succeeded after previous failures")
			}
			return nil
		}
		retryCount++

		if !isRetriable(err) {
			log.Info().Err(err).Str("op", tryFuncLabel).Str("next op", onNonRetriableLabel).Msg("error is not retriable so we will try the next method if available.")
			return onNonRetriable(err)
		} else {
			log.Info().Err(err).Str("op", tryFuncLabel).Str("next op", onNonRetriableLabel).Int("retries", retryCount).Msg("error is retriable so we will try it again after the sleep")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(backoff)
			if backoff <= 4*time.Second {
				backoff *= 2
			}
		}
	}
}
