package maintainer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestStopWaitsForLoopAndAllowsCleanRestart(t *testing.T) {
	service := New(nil, nil, nil)
	service.Interval = time.Hour

	releaseFirstRun := make(chan struct{})
	runEntered := make(chan int32, 2)
	var runs atomic.Int32
	service.Enabled = func() bool {
		run := runs.Add(1)
		runEntered <- run
		if run == 1 {
			<-releaseFirstRun
		}
		return false
	}

	service.Start()
	service.RequestRunSoon(false)
	waitForMaintainerRun(t, runEntered, 1)

	stopDone := make(chan struct{})
	go func() {
		service.Stop()
		close(stopDone)
	}()

	returnedEarly := false
	select {
	case <-stopDone:
		returnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirstRun)
	waitForMaintainerStop(t, stopDone)
	if returnedEarly {
		t.Fatal("Stop returned while the active loop was still running")
	}

	service.Start()
	service.RequestRunSoon(false)
	waitForMaintainerRun(t, runEntered, 2)
	service.Stop()

	if got := runs.Load(); got != 2 {
		t.Fatalf("runs after restart = %d, want 2", got)
	}
}

func waitForMaintainerRun(t *testing.T, runs <-chan int32, want int32) {
	t.Helper()
	select {
	case got := <-runs:
		if got != want {
			t.Fatalf("run = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for run %d", want)
	}
}

func waitForMaintainerStop(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Stop")
	}
}
