package raft

import (
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"time"

	"github.com/charmbracelet/log"
	"github.com/pelageech/go-raft/internal/journal"
	"github.com/pelageech/go-raft/internal/raft/sms"

	"github.com/google/uuid"
)

type (
	ID   fmt.Stringer
	Role int
	SMS  = sms.Message
)

const (
	Follower Role = iota
	Candidate
	Leader
)

type Node struct {
	id                  ID
	term                int
	role                Role
	nodes               map[ID]*Node
	voted               bool
	currentVotes        int
	votePool            map[ID]bool
	maxDelta            time.Duration
	leaderHeartDeadline time.Time
	messages            chan SMS
	updaters            chan string
	indexPool           map[ID]*time.Ticker
	nodePoolWait        map[ID]chan struct{}

	journal *journal.Journal

	logger *log.Logger

	// debug only
	turnOff chan struct{}
}

const _messageBufferSise = 1000
const _factor = 1

func NewNode(nodes iter.Seq[*Node]) *Node {
	n := &Node{
		id:                  uuid.New(),
		journal:             journal.NewJournal(),
		term:                -1,
		role:                Follower,
		nodes:               make(map[ID]*Node),
		votePool:            make(map[ID]bool),
		messages:            make(chan SMS, _messageBufferSise),
		updaters:            make(chan string, _messageBufferSise),
		logger:              log.New(os.Stdout),
		maxDelta:            randDelta(),
		leaderHeartDeadline: time.Now().Add(rand.N(5 * time.Second)),
		turnOff:             make(chan struct{}, 1),
		nodePoolWait:        make(map[ID]chan struct{}, 1),
		indexPool:           make(map[ID]*time.Ticker),
	}
	for node := range nodes {
		n.nodes[node.id] = node
		n.indexPool[node.id] = time.NewTicker(time.Second / _factor)
	}
	return n
}

func (n *Node) ID() ID {
	return n.id
}

func (n *Node) Term() int {
	return n.term
}

func (n *Node) JournalLen() int {
	return n.journal.Len()
}

func (n *Node) Role() Role {
	return n.role
}

func (n *Node) Run(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("id:%v, panic: %v", n.id, r))
		}
	}()
	ticker := time.NewTicker(time.Second / _factor)

loop:
	for {
		n.turnOff <- struct{}{}
		<-n.turnOff

		select {
		case <-ctx.Done():
			break loop
		case msg := <-n.messages:
			now := time.Now()
			n.logger.Infof("%v: got sms with message `%s`", n.ID(), msg)
			if msg.GetTo() != n.ID() {
				break
			}
			switch v := msg.(type) {
			case sms.RequestVote:
				n.requestVoteHandle(v, now)
			case sms.Vote:
				n.voteHandler(v)
			case sms.AppendEntries:
				n.appendEntriesHandler(v, now)
			case sms.AppendEntriesResponse:
				if n.role != Leader {
					continue
				}
				go func() {
					select {
					case <-n.indexPool[msg.GetFrom()].C:
						n.appendEntriesResponseHandler(v, now)
					case <-ctx.Done():
					}
				}()
			}
		case <-ticker.C:
			now := time.Now()
			// if n.role == Leader {
			// 	n.heartBeat()
			// 	break
			// }

			if n.role == Candidate {
				n.retryRequestVotes()
				break
			}

			if n.IsLeaderDead(now) {
				n.SetRole(Candidate)
				go n.Election(now)
				break
			}
		}
	}
	return nil
}

func (n *Node) IsLeaderDead(timeNow time.Time) bool {
	return !n.leaderHeartDeadline.IsZero() && n.leaderHeartDeadline.Before(timeNow)
}

func (n *Node) Send(sms SMS) error {
	n.logger.Infof("%v: send sms `%s`", n.ID(), sms)
	n.messages <- sms
	return nil
}

func (n *Node) Election(timeNow time.Time) {
	n.logger.Infof("%v: election", n.ID())
	n.currentVotes = 0
	n.clearVotePool()
	n.updateTerm(n.term+1, timeNow)
	for _, node := range n.nodes {
		_ = node.Send(sms.RequestVote{
			From: n.ID().String(),
			To:   node.ID().String(),
			Term: n.term,
		})
	}
}

func (n *Node) SetRole(role Role) {
	n.role = role
}

func (n *Node) Add(node *Node) error {
	if _, ok := n.nodes[node.id]; ok {
		return fmt.Errorf("node `%v` already exists", node.id)
	}
	n.nodes[node.id] = node
	n.votePool[node.ID()] = false
	n.indexPool[node.ID()] = time.NewTicker(time.Second / _factor)

	return nil
}

func (n *Node) clearVotePool() {
	for id := range n.votePool {
		n.votePool[id] = false
	}
}

func (n *Node) retryRequestVotes() {
	for id := range n.votePool {
		if n.voted {
			continue
		}
		_ = n.nodes[id].Send(sms.RequestVote{
			From: n.ID().String(),
			To:   id.String(),
			Term: n.term,
		})
	}
}

func (n *Node) addDeadline2(timeNow time.Time) {
	delta := n.leaderHeartDeadline.Sub(timeNow)
	if (n.maxDelta-delta)/4 == 0 {
		return
	}
	r := rand.N(2*time.Second) / _factor * 4
	n.leaderHeartDeadline = n.leaderHeartDeadline.Add(r)
}

func randDelta() time.Duration {
	return 4*time.Second + rand.N(4*time.Second)
}

func (n *Node) updateTerm(term int, timeNow time.Time) {
	if n.term > term {
		return
	}
	if n.term == term {
		n.addDeadline2(timeNow)
	}
	n.term = term
	n.SetRole(Follower)
	n.maxDelta = randDelta()
	n.leaderHeartDeadline = timeNow.Add(n.maxDelta)
}

func (n *Node) Request(s string) {
	n.updaters <- s
}
