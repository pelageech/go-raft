package raft

import (
	"time"

	"github.com/pelageech/go-raft/internal/journal"
	"github.com/pelageech/go-raft/internal/raft/sms"
)

func (n *Node) requestVoteHandle(msg sms.RequestVote, timeNow time.Time) {
	to := n.nodes[msg.GetFrom()]

	if msg.GetTerm() <= n.term { // if we don't need to update term
		_ = to.Send(sms.Vote{
			From:        n.ID().String(),
			To:          to.ID().String(),
			Term:        n.term,
			VoteGranted: false,
		})
		return
	}

	granted := true

	if n.voted {
		granted = false
	}
	n.voted = true

	n.updateTerm(msg.GetTerm(), timeNow)

	vote := sms.Vote{
		From:        n.ID().String(),
		To:          to.ID().String(),
		Term:        n.term,
		VoteGranted: granted,
	}

	_ = to.Send(vote)
}

func (n *Node) voteHandler(msg sms.Vote) {
	if n.role == Leader {
		return
	}
	n.votePool[msg.GetFrom()] = true
	if msg.Term != n.term {
		return
	}

	if msg.VoteGranted {
		n.currentVotes++
	}
	n.logger.Infof("%v: got `%d`", n.ID(), n.currentVotes)
	if n.currentVotes >= (len(n.votePool)+1)/2 {
		n.logger.Infof("a leader is %v", n.ID())
		n.SetRole(Leader)
		n.leaderHeartDeadline = time.Time{}
		for _, node := range n.nodes {
			_ = node.Send(sms.AppendEntries{
				From:        n.ID().String(),
				To:          node.ID().String(),
				Term:        n.term,
				PrevIndex:   n.journal.PrevIndex(),
				PrevTerm:    n.journal.Get(n.journal.PrevIndex()).Term,
				CommitIndex: n.journal.CommitIndex(),
				Entries:     nil,
			})
		}
	}
}

func (n *Node) heartBeatHandler(msg sms.HeartBeat, timeNow time.Time) {
	if n.term == msg.Term && n.role == Leader {
		panic("there are two leaders!")
	}
	n.voted = false
	n.updateTerm(msg.GetTerm(), timeNow)

	n.SetRole(Follower)
}

func (n *Node) appendEntriesHandler(msg sms.AppendEntries, timeNow time.Time) {
	n.updateTerm(msg.GetTerm(), timeNow)
	n.voted = false
	n.SetRole(Follower)

	if n.term < msg.Term {
		n.term = msg.Term
	}
	if msg.CommitIndex > n.journal.CommitIndex() {
		if !n.journal.Commit() && msg.PrevIndex > n.journal.PrevIndex() {
			_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntriesResponse{
				From:       n.ID().String(),
				To:         msg.From,
				Term:       n.term,
				Success:    false,
				MatchIndex: n.journal.CommitIndex(),
			})
			return
		}
	}
	if msg.CommitIndex == n.journal.CommitIndex() && n.journal.Get(msg.CommitIndex).Term == msg.PrevTerm {
		if len(msg.Entries) > 0 {
			_ = n.journal.Put(journal.Message{
				Term:  msg.Term,
				Index: n.journal.Len(),
				Data:  []byte(msg.Entries[0].Data),
			})
		}
		_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntriesResponse{
			From:       n.ID().String(),
			To:         msg.From,
			Term:       n.term,
			Success:    true,
			MatchIndex: n.journal.PrevIndex(),
		})
		return
	}
	n.logger.Warn("FDFDF")
	_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntriesResponse{
		From:       n.ID().String(),
		To:         msg.From,
		Term:       n.term,
		Success:    false,
		MatchIndex: msg.PrevIndex,
	})
}

func (n *Node) appendEntriesResponseHandler(msg sms.AppendEntriesResponse, timeNow time.Time) {
	if msg.Success {
		if msg.MatchIndex == n.journal.PrevIndex() && n.journal.PrevIndex() != n.journal.CommitIndex() {
			n.currentVotes++
			if n.currentVotes >= (len(n.votePool)+1)/2 {
				n.logger.Infof("committed %v", n.journal.Last())
				n.journal.Commit()
				_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntries{
					From:        n.ID().String(),
					To:          msg.From,
					Term:        n.term,
					PrevIndex:   n.journal.PrevIndex(),
					PrevTerm:    n.journal.Get(msg.MatchIndex).Term,
					CommitIndex: n.journal.CommitIndex(),
					Entries:     nil,
				})
				return
			}
		}
		if n.journal.CommitIndex() > msg.MatchIndex {
			_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntries{
				From:        n.ID().String(),
				To:          msg.From,
				Term:        n.term,
				PrevIndex:   msg.MatchIndex + 1,
				PrevTerm:    n.journal.Get(msg.MatchIndex).Term,
				CommitIndex: msg.MatchIndex + 1,
				Entries: []sms.Entry[string]{
					{
						Data: string(n.journal.Get(msg.MatchIndex + 1).Data),
						Term: n.journal.Get(msg.MatchIndex + 1).Term,
					},
				},
			})
			return
		}
		var entry []sms.Entry[string]
		select {
		case v := <-n.updaters:
			entry = append(entry, sms.Entry[string]{
				Data: v,
				Term: n.term,
			})
			_ = n.journal.Put(journal.Message{
				Term:  n.term,
				Index: n.journal.Len(),
				Data:  []byte(v),
			})
		default:
		}
		n.logger.Infof("%v", n.journal.Len())
		_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntries{
			From:        n.ID().String(),
			To:          msg.From,
			Term:        n.term,
			PrevIndex:   msg.MatchIndex,
			PrevTerm:    n.journal.Get(msg.MatchIndex).Term,
			CommitIndex: n.journal.CommitIndex(),
			Entries:     entry,
		})
		return
	}
	_ = n.nodes[msg.GetFrom()].Send(sms.AppendEntries{
		From:        n.ID().String(),
		To:          msg.From,
		Term:        n.term,
		PrevIndex:   msg.MatchIndex - 1,
		PrevTerm:    n.journal.Get(msg.MatchIndex).Term,
		CommitIndex: n.journal.CommitIndex(),
		Entries: []sms.Entry[string]{
			{
				Data: string(n.journal.Get(msg.MatchIndex).Data),
				Term: n.journal.Get(msg.MatchIndex).Term,
			},
		},
	})
}
