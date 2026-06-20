package model

import (
	"sync"
	"time"
)

type NodeStatus string

const (
	NodeStatusOnline  NodeStatus = "online"
	NodeStatusOffline NodeStatus = "offline"
)

type Node struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Address   string     `json:"address"`
	Weight    int        `json:"weight"`
	Status    NodeStatus `json:"status"`
	TaskCount int        `json:"task_count"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type NodeStore struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	order []string
}

func NewNodeStore() *NodeStore {
	return &NodeStore{
		nodes: make(map[string]*Node),
		order: make([]string, 0),
	}
}

func (s *NodeStore) Add(node *Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.nodes[node.ID]; !exists {
		s.order = append(s.order, node.ID)
	}
	s.nodes[node.ID] = node
}

func (s *NodeStore) Get(id string) (*Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

func (s *NodeStore) Update(id string, fn func(*Node)) (*Node, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, false
	}
	fn(n)
	n.UpdatedAt = time.Now()
	cp := *n
	return &cp, true
}

func (s *NodeStore) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[id]; !ok {
		return false
	}
	delete(s.nodes, id)
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return true
}

func (s *NodeStore) List() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Node, 0, len(s.order))
	for _, id := range s.order {
		cp := *s.nodes[id]
		result = append(result, &cp)
	}
	return result
}

func (s *NodeStore) OnlineNodes() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Node, 0)
	for _, id := range s.order {
		n := s.nodes[id]
		if n.Status == NodeStatusOnline {
			cp := *n
			result = append(result, &cp)
		}
	}
	return result
}

func (s *NodeStore) IncrementTaskCount(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.nodes[id]; ok {
		n.TaskCount++
		n.UpdatedAt = time.Now()
	}
}

func (s *NodeStore) DecrementTaskCount(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.nodes[id]; ok {
		if n.TaskCount > 0 {
			n.TaskCount--
		}
		n.UpdatedAt = time.Now()
	}
}
