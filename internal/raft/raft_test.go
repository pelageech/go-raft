package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaft(t *testing.T) {
	raft, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		_ = raft.Run(ctx)
	}()
	time.Sleep(3 * time.Second)

	var leader *Node
	require.Eventually(t, func() bool {
		leader = findLeader(raft)
		return leader != nil
	}, 5*time.Second, 100*time.Millisecond)

	firstLeader := leader
	t.Log(firstLeader.ID())

	// turn off first leader
	firstLeader.turnOff <- struct{}{}
	time.Sleep(15 * time.Second)
	require.Eventually(t, func() bool {
		leader = findLeader(raft)
		return leader != nil
	}, 5*time.Second, 100*time.Millisecond)

	secondLeader := leader
	t.Log(secondLeader.ID())

	require.NotEqual(t, firstLeader.ID(), secondLeader.ID())
	require.NotEqual(t, firstLeader.Term(), secondLeader.Term())

	// turn on first leader
	<-firstLeader.turnOff

	require.Eventually(t, func() bool {
		return firstLeader.Term() == secondLeader.Term()
	}, 2*time.Second, 100*time.Millisecond)

	cancel()
	<-done
}

func TestLog(t *testing.T) {
	raft, err := New(5)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		_ = raft.Run(ctx)
	}()
	time.Sleep(3 * time.Second)

	var leader *Node
	require.Eventually(t, func() bool {
		leader = findLeader(raft)
		return leader != nil
	}, 5*time.Second, 100*time.Millisecond)
	t.Log("put aboba")
	leader.Request("aboba")
	first := leader
	time.Sleep(3 * time.Second)
	leader.turnOff <- struct{}{}
	time.Sleep(7 * time.Second)
	require.Eventually(t, func() bool {
		leader = findLeader(raft)
		return leader != nil
	}, 5*time.Second, 100*time.Millisecond)
	leader.Request("aboba2")
	time.Sleep(3 * time.Second)
	<-first.turnOff
	time.Sleep(3 * time.Second)
	for _, node := range raft.nodes {
		t.Log(node.journal.Get(1))
		t.Log(node.journal.Len())
	}
	cancel()
	<-done
}

func findLeader(raft *Raft) (n *Node) {
	maxTerm := -2
	for _, node := range raft.nodes {
		if node.Role() == Leader && node.Term() > maxTerm {
			n = node
			maxTerm = node.Term()
		}
	}
	return
}
