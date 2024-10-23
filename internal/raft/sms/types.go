package sms

import (
	"fmt"
	"github.com/google/uuid"
)

type Message interface {
	fmt.Stringer
	GetTerm() int
	GetFrom() uuid.UUID
	GetTo() uuid.UUID
	Type() string
}

type RequestVote struct {
	From string `json:"from"`
	To   string `json:"to"`
	Term int    `json:"term"`
}

func (r RequestVote) GetTerm() int {
	return r.Term
}

func (r RequestVote) GetFrom() uuid.UUID {
	return uuid.MustParse(r.From)
}

func (r RequestVote) GetTo() uuid.UUID {
	return uuid.MustParse(r.To)
}

func (r RequestVote) Type() string {
	return "RequestVote"
}

func (r RequestVote) String() string {
	return fmt.Sprintf("RequestVote{from %s to %s}, Term is %d", r.From, r.To, r.Term)
}

type Vote struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func (v Vote) GetTerm() int {
	return v.Term
}

func (v Vote) GetFrom() uuid.UUID {
	return uuid.MustParse(v.From)
}

func (v Vote) GetTo() uuid.UUID {
	return uuid.MustParse(v.To)
}

func (v Vote) Type() string {
	return "Vote"
}

func (v Vote) String() string {
	return fmt.Sprintf("Vote{from %s to %s, granted=%t}, Term is %d", v.From, v.To, v.VoteGranted, v.Term)
}
