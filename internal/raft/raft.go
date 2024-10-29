package raft

import (
	"context"
	"slices"

	"golang.org/x/sync/errgroup"
)

type Raft struct {
	nodes []*Node
}

func NewWithNodes(nodeSet ...*Node) *Raft {
	nodes := make([]*Node, len(nodeSet))
	for i, nodeFromSet := range nodeSet {
		nodes[i] = nodeFromSet
		for _, nd := range nodes[:i] { // for each prev nodes add new one
			_ = nd.Add(nodes[i])
		}
	}
	return &Raft{nodes}
}

func New(n int) (*Raft, error) {
	nodes := make([]*Node, n)
	for i := range n {
		nodes[i] = NewNode(slices.Values(nodes[:i]))
		for _, nd := range nodes[:i] {
			if err := nd.Add(nodes[i]); err != nil {
				return nil, err
			}
		}
	}
	return &Raft{nodes}, nil
}

func (r *Raft) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, n := range r.nodes {
		g.Go(func() error {
			return n.Run(ctx)
		})
	}
	return g.Wait()
}
