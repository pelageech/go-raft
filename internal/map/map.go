package raftmap

import "errors"

type Map[K comparable, V any] struct {
	m map[K]V
}

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{m: make(map[K]V)}
}

var ErrInvalidRequest = errors.New("invalid request")

func (m *Map[K, V]) Process(s any) (any, error) {
	switch v := s.(type) {
	case map[string]any:
		kk, ok1 := v["key"]
		vv, ok2 := v["value"]
		if ok1 && ok2 {
			k, ok := kk.(K)
			if !ok {
				return "", ErrInvalidRequest
			}
			v, ok := vv.(V)
			if !ok {
				return "", ErrInvalidRequest
			}

			m.m[k] = v
			return "", nil
		}
	}
	return "", ErrInvalidRequest
}

func (m *Map[K, V]) Dump() map[K]V {
	return m.m
}

func (m *Map[K, V]) Get(k K) (V, bool) {
	v, ok := m.m[k]
	return v, ok
}
