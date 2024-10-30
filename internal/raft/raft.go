package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
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

func (r *Raft) Node(id ID) *Node {
	for _, n := range r.nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

type Handler struct {
	raft *Raft
}

func NewHandler(raft *Raft) *Handler {
	return &Handler{raft}
}

func (h *Handler) Nodes(w http.ResponseWriter, r *http.Request) {
	buf := &bytes.Buffer{}
	for i, n := range h.raft.nodes {
		buf.WriteString(strconv.Itoa(i+1) + ": " + n.ID().String() + " | Role:" + n.Role().String() + " | Term:" + strconv.Itoa(n.Term()) + " | JournalLen: " + strconv.Itoa(n.JournalLen()) + "\n")
	}

	_, err := io.Copy(w, buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (n *Handler) Journal(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := n.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	buf := &bytes.Buffer{}
	buf.WriteString("Journal of " + node.ID().String() + ":\n")
	for entry := range node.journal.Entries() {
		buf.WriteString(entry.String() + "\n")
	}

	_, err = io.Copy(w, buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type request struct {
	Msg map[string]any `json:"msg"`
	ID  string         `json:"id"`
}

func (h *Handler) Request(w http.ResponseWriter, r *http.Request) {
	var req request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(req.ID)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := h.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	node.Request(req.Msg)

	_, _ = w.Write([]byte(fmt.Sprint(req)))
}

func (h *Handler) Kill(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := h.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	node.turnOff <- struct{}{}
}

func (h *Handler) Recover(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := h.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	<-node.turnOff
}

func (h *Handler) DumpMap(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := h.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	buf := &bytes.Buffer{}
	buf.WriteString("Map of " + node.ID().String() + ":\n")
	buf.WriteString(fmt.Sprint(node.journal.Proc().Dump()))

	_, err = io.Copy(w, buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	node := h.raft.Node(ID(uid))
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	v, ok := node.journal.Proc().Get(r.URL.Query().Get("key"))
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	_, err = io.Copy(w, strings.NewReader(fmt.Sprint(v)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
