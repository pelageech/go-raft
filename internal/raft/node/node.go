package node

import (
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"sync"
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
	voteCond            *sync.Cond
	votePool            map[ID]bool
	leaderHeartDeadline time.Time
	messages            chan SMS

	journal *journal.Journal

	logger *log.Logger
}

const _messageBufferSise = 1000

func NewNode(nodes iter.Seq[*Node]) *Node {
	n := &Node{
		id:                  uuid.New(),
		journal:             journal.NewJournal(),
		term:                -1,
		role:                Follower,
		nodes:               make(map[ID]*Node),
		votePool:            make(map[ID]bool),
		voteCond:            sync.NewCond(&sync.Mutex{}),
		messages:            make(chan SMS, _messageBufferSise),
		logger:              log.New(os.Stdout),
		leaderHeartDeadline: time.Now().Add(rand.N(10 * time.Second)),
	}
	for node := range nodes {
		n.nodes[node.id] = node
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
	ticker := time.NewTicker(time.Second / 16)
	for {
		select {
		case <-ctx.Done():
			break
		case msg := <-n.messages:
			n.logger.Infof("%v: got sms with message `%s`", n.ID(), msg)
			if msg.GetTo() != n.ID() {
				break
			}

			to := n.nodes[msg.GetFrom()]

			switch v := msg.(type) {
			case sms.RequestVote:
				if n.voted {
					break
				}
				n.voted = true
				granted := true
				if v.Term <= n.term {
					granted = false
				}
				n.term = msg.GetTerm()

				vote := sms.Vote{
					From:        n.ID().String(),
					To:          to.ID().String(),
					Term:        n.term,
					VoteGranted: granted,
				}

				n.logger.Infof("%v: send vote for `%s`", n.ID(), to.ID())
				_ = to.Send(vote)
			case sms.Vote:
				if n.role == Leader {
					break
				}
				if v.Term != n.term {
					panic(fmt.Sprintf("vote term `%v`:%d != `%v`:%d", v.GetFrom(), v.Term, n.ID(), n.term))
				}
				n.votePool[v.GetFrom()] = true
				if v.VoteGranted {
					n.voteCond.L.Lock()
					n.currentVotes++
					n.voteCond.L.Unlock()
					n.voteCond.Signal()
				}
				n.logger.Infof("%v: got `%d`", n.ID(), n.currentVotes)
				if n.currentVotes >= (len(n.votePool)+1)/2 {
					n.logger.Infof("a leader is %v", n.ID())
					n.SetRole(Leader)
				}
			}
		case <-ticker.C:
			if n.IsLeaderDead(time.Now()) {
				n.leaderHeartDeadline = time.Now().Add(500 * time.Second)
				n.SetRole(Candidate)
				go n.Election()
			}
		}
	}
	return nil
}

func (n *Node) IsLeaderDead(timeNow time.Time) bool {
	return n.leaderHeartDeadline.Before(timeNow)
}

func (n *Node) Send(sms SMS) error {
	n.messages <- sms
	return nil
}

func (n *Node) Election() {
	n.logger.Infof("%v: election", n.ID())
	n.term++
	n.currentVotes = 0
	n.voted = false
	n.clearVotePool()
	for _, node := range n.nodes {
		_ = node.Send(sms.RequestVote{
			From: n.ID().String(),
			To:   node.ID().String(),
			Term: n.term,
		})
	}
	n.voteCond.L.Lock()
	for n.role == Candidate {
		n.voteCond.Wait()
	}
	n.voteCond.L.Unlock()
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

	return nil
}

func (n *Node) clearVotePool() {
	for id := range n.votePool {
		n.votePool[id] = false
	}
}
