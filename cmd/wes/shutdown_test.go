package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
)

// ADR-0023 §8.3: gracefulShutdown must wait for the Kafka consumer's Run to
// return BEFORE it tears down anything the consumer uses.
func TestGracefulShutdown_WaitsForConsumerRunBeforeTearingDownDependencies(t *testing.T) {
	var consumerReturned atomic.Bool
	var returnedBeforeTeardown atomic.Bool

	s := &serving{
		logger:   slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		server:   &http.Server{},
		handlers: &inboundhttp.Handlers{Readiness: &inboundhttp.Readiness{}},
		// First dependency teardown after the consumer wait.
		cancelCatalogueConsumer: func() { returnedBeforeTeardown.Store(consumerReturned.Load()) },
	}

	consumerDone := make(chan struct{})
	relayDone := make(chan struct{})
	close(relayDone)
	go func() {
		time.Sleep(150 * time.Millisecond) // consumer is mid-message when cancelled
		consumerReturned.Store(true)
		close(consumerDone)
	}()

	var cancelled atomic.Bool
	if err := s.gracefulShutdown(func() { cancelled.Store(true) }, nil, consumerDone, func() {}, relayDone); err != nil {
		t.Fatalf("gracefulShutdown: %v", err)
	}
	if !cancelled.Load() {
		t.Fatal("the consumer context must be cancelled")
	}
	if !returnedBeforeTeardown.Load() {
		t.Fatal("dependencies were torn down before consumer.Run returned")
	}
}

func TestWaitDone(t *testing.T) {
	t.Run("returns when done closes", func(t *testing.T) {
		logs := &bytes.Buffer{}
		done := make(chan struct{})
		close(done)
		waitDone(context.Background(), slog.New(slog.NewTextHandler(logs, nil)), "x", done)
		if logs.Len() != 0 {
			t.Fatalf("unexpected log: %s", logs)
		}
	})
	t.Run("warns when the deadline expires first", func(t *testing.T) {
		logs := &bytes.Buffer{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		waitDone(ctx, slog.New(slog.NewTextHandler(logs, nil)), "kafka consumer", make(chan struct{}))
		if !strings.Contains(logs.String(), "kafka consumer did not stop before the shutdown deadline") {
			t.Fatalf("missing warning: %s", logs)
		}
	})
}
