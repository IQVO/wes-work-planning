package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// A consumer whose Run returns while it was NOT asked to stop is dead: the
// process must exit non-zero (run() returns the error, the pod restarts)
// instead of staying alive with a dead consumer.
func TestConsumerStopped_ReportsADeathThatWasNotAskedFor(t *testing.T) {
	fatal := make(chan error, 1)
	s := &serving{logger: quietLogger(), fatal: fatal}

	s.consumerStopped(context.Background(), "kafka consumer", errors.New("commit failed"))
	select {
	case err := <-fatal:
		if !strings.Contains(err.Error(), "kafka consumer stopped on its own") || !strings.Contains(err.Error(), "commit failed") {
			t.Fatalf("fatal = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a consumer that died on its own was not reported")
	}

	s.consumerStopped(context.Background(), "kafka consumer", nil)
	select {
	case err := <-fatal:
		if !strings.Contains(err.Error(), "returned without being asked to stop") {
			t.Fatalf("fatal = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a consumer that returned nil while live was not reported")
	}
}

// A stop requested through the context is the normal shutdown, never fatal.
func TestConsumerStopped_IgnoresARequestedStop(t *testing.T) {
	fatal := make(chan error, 1)
	s := &serving{logger: quietLogger(), fatal: fatal}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.consumerStopped(ctx, "kafka consumer", nil)
	s.consumerStopped(ctx, "kafka consumer", context.Canceled)
	s.consumerStopped(ctx, "kafka consumer", errors.New("close error during shutdown"))
	select {
	case err := <-fatal:
		t.Fatalf("a requested stop was reported as fatal: %v", err)
	default:
	}
}

// With no listener for deaths (a unit test) and a full channel the reporter
// never blocks.
func TestReportFatal_NeverBlocks(t *testing.T) {
	(&serving{}).reportFatal(errors.New("x")) // nil channel: dropped
	fatal := make(chan error, 1)
	s := &serving{fatal: fatal}
	s.reportFatal(errors.New("first"))
	done := make(chan struct{})
	go func() { s.reportFatal(errors.New("second")); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reportFatal blocked on a full channel")
	}
}
