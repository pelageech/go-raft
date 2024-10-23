package journal

import "sync"

type Message any

type Journal struct {
	mu      sync.RWMutex
	storage []Message
}

func NewJournal() *Journal {
	return &Journal{}
}

func (j *Journal) Put(m Message) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.storage = append(j.storage, m)
}

func (j *Journal) Len() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return len(j.storage)
}
