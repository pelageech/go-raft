package journal

import "errors"

type Message struct {
	Term  int
	Index int
	Data  []byte
}

type Journal struct {
	storage     []Message
	commitIndex int
}

func NewJournal() *Journal {
	return &Journal{commitIndex: 0, storage: []Message{
		{
			Term: -1,
			Data: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
	}}
}

func (j *Journal) Put(m Message) error {
	// first message
	if j.Len() == 0 {
		j.storage = append(j.storage, m)
		return nil
	}

	// append
	if j.Len() == m.Index {
		if j.storage[len(j.storage)-1].Term > m.Term {
			return errors.New("term is greater than msg term")
		}
		j.storage = append(j.storage, m)
		return nil
	}

	// put in the middle
	if j.storage[m.Index].Term >= m.Term {
		return errors.New("term is greater than msg term")
	}
	j.storage[m.Index] = m
	return nil
}

func (j *Journal) Commit() bool {
	if j.commitIndex == len(j.storage) {
		return false
	}
	j.commitIndex++
	return true
}

func (j *Journal) CommitIndex() int {
	return j.commitIndex
}

func (j *Journal) Len() int {
	return len(j.storage)
}

func (j *Journal) PrevIndex() int {
	return j.Len() - 1
}

func (j *Journal) PrevTerm() int {
	return j.storage[j.PrevIndex()].Term
}

func (j *Journal) Get(i int) Message {
	return j.storage[i]
}

func (j *Journal) Last() Message {
	return j.storage[len(j.storage)-1]
}
