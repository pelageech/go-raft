package raft

import (
	"context"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	raft, err := New(5)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = raft.Run(ctx)
	t.Log(err)
}
