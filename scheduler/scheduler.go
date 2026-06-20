package scheduler

import (
	"fmt"
	"sync"

	"task-scheduler/model"
)

type WeightedNode struct {
	Node          *model.Node
	CurrentWeight int
	Weight        int
}

type WeightedRoundRobin struct {
	mu       sync.Mutex
	nodes    map[string]*WeightedNode
	nodeList []*WeightedNode
}

func NewWeightedRoundRobin() *WeightedRoundRobin {
	return &WeightedRoundRobin{
		nodes:    make(map[string]*WeightedNode),
		nodeList: make([]*WeightedNode, 0),
	}
}

func (w *WeightedRoundRobin) AddNode(node *model.Node) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.nodes[node.ID]; exists {
		wn := w.nodes[node.ID]
		wn.Weight = node.Weight
		wn.Node = node
		return
	}
	wn := &WeightedNode{
		Node:          node,
		CurrentWeight: 0,
		Weight:        node.Weight,
	}
	w.nodes[node.ID] = wn
	w.nodeList = append(w.nodeList, wn)
}

func (w *WeightedRoundRobin) RemoveNode(nodeID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.nodes, nodeID)
	for i, wn := range w.nodeList {
		if wn.Node.ID == nodeID {
			w.nodeList = append(w.nodeList[:i], w.nodeList[i+1:]...)
			break
		}
	}
}

func (w *WeightedRoundRobin) UpdateWeight(nodeID string, weight int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	wn, ok := w.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	wn.Weight = weight
	wn.Node.Weight = weight
	return nil
}

func (w *WeightedRoundRobin) Next() (*model.Node, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.nodeList) == 0 {
		return nil, fmt.Errorf("no available nodes")
	}

	var best *WeightedNode
	totalWeight := 0

	for _, wn := range w.nodeList {
		if wn.Node.Status != model.NodeStatusOnline {
			continue
		}
		wn.CurrentWeight += wn.Weight
		totalWeight += wn.Weight
		if best == nil || wn.CurrentWeight > best.CurrentWeight {
			best = wn
		}
	}

	if best == nil {
		return nil, fmt.Errorf("no online nodes available")
	}

	best.CurrentWeight -= totalWeight

	result := *best.Node
	return &result, nil
}

func (w *WeightedRoundRobin) Sync(nodes []*model.Node) {
	w.mu.Lock()
	defer w.mu.Unlock()

	activeIDs := make(map[string]bool)
	newList := make([]*WeightedNode, 0, len(nodes))

	for _, n := range nodes {
		activeIDs[n.ID] = true
		if wn, exists := w.nodes[n.ID]; exists {
			wn.Node = n
			wn.Weight = n.Weight
			newList = append(newList, wn)
		} else {
			wn := &WeightedNode{
				Node:          n,
				CurrentWeight: 0,
				Weight:        n.Weight,
			}
			w.nodes[n.ID] = wn
			newList = append(newList, wn)
		}
	}

	for id := range w.nodes {
		if !activeIDs[id] {
			delete(w.nodes, id)
		}
	}

	w.nodeList = newList
}

func (w *WeightedRoundRobin) Stats() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	stats := make(map[string]int)
	for _, wn := range w.nodeList {
		stats[wn.Node.ID] = wn.CurrentWeight
	}
	return stats
}
