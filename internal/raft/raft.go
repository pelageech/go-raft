package raft

import (
	"context"
	"slices"

	"github.com/pelageech/go-raft/internal/raft/node"

	"golang.org/x/sync/errgroup"
)

type Raft struct {
	nodes []*node.Node
}

func New(n int) (*Raft, error) {
	nodes := make([]*node.Node, n)
	for i := range n {
		nodes[i] = node.NewNode(slices.Values(nodes[:i]))
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
