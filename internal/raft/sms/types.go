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

var _ Message = &RequestVote{}

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

var _ Message = Vote{}

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

var _ Message = HeartBeat{}

type HeartBeat struct {
	From string `json:"from"`
	To   string `json:"to"`
	Term int    `json:"term"`
}

func (v HeartBeat) GetTerm() int {
	return v.Term
}

func (v HeartBeat) GetFrom() uuid.UUID {
	return uuid.MustParse(v.From)
}

func (v HeartBeat) GetTo() uuid.UUID {
	return uuid.MustParse(v.To)
}

func (v HeartBeat) Type() string {
	return "HeartBeat"
}

func (v HeartBeat) String() string {
	return fmt.Sprintf("HeartBeat{from %s to %s}, Term is %d", v.From, v.To, v.Term)
}

type Entry[T any] struct {
	Data T   `json:"data"`
	Term int `json:"term"`
}

type AppendEntries struct {
	From        string       `json:"from"`
	To          string       `json:"to"`
	Term        int          `json:"term"`
	PrevIndex   int          `json:"prev_index"`
	PrevTerm    int          `json:"prev_term"`
	CommitIndex int          `json:"commit_index"`
	Entries     []Entry[any] `json:"entries"`
}

func (v AppendEntries) GetTerm() int {
	return v.Term
}

func (v AppendEntries) GetFrom() uuid.UUID {
	return uuid.MustParse(v.From)
}

func (v AppendEntries) GetTo() uuid.UUID {
	return uuid.MustParse(v.To)
}

func (v AppendEntries) Type() string {
	return "AppendEntries"
}

func (v AppendEntries) String() string {
	return fmt.Sprintf("AppendEntries{from %s to %s}, Term is %d, PrevIndex=%d, PrevTerm=%d, Commit=%d, Len=%d", v.From, v.To, v.Term, v.PrevIndex, v.PrevTerm, v.CommitIndex, len(v.Entries))
}

type AppendEntriesResponse struct {
	From       string `json:"from"`
	To         string `json:"to"`
	Term       int    `json:"term"`
	Success    bool   `json:"success"`
	MatchIndex int    `json:"match_index"`
}

func (v AppendEntriesResponse) GetTerm() int {
	return v.Term
}

func (v AppendEntriesResponse) GetFrom() uuid.UUID {
	return uuid.MustParse(v.From)
}

func (v AppendEntriesResponse) GetTo() uuid.UUID {
	return uuid.MustParse(v.To)
}

func (v AppendEntriesResponse) Type() string {
	return "AppendEntriesResponse"
}

func (v AppendEntriesResponse) String() string {
	return fmt.Sprintf("AppendEntriesResponse{from %s to %s}, Term is %d, Success=%t, Match=%d", v.From, v.To, v.Term, v.Success, v.MatchIndex)
}
