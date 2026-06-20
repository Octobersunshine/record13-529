package model

import (
	"sync"
	"time"
)

type LoadReport struct {
	NodeID      string    `json:"node_id"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemUsage    float64   `json:"mem_usage"`
	LoadAvg1    float64   `json:"load_avg_1"`
	LoadAvg5    float64   `json:"load_avg_5"`
	LoadAvg15   float64   `json:"load_avg_15"`
	ActiveConns int       `json:"active_conns"`
	ReportedAt  time.Time `json:"reported_at"`
}

type LoadStore struct {
	mu      sync.RWMutex
	reports map[string]*LoadReport
}

func NewLoadStore() *LoadStore {
	return &LoadStore{
		reports: make(map[string]*LoadReport),
	}
}

func (s *LoadStore) Update(report *LoadReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	report.ReportedAt = time.Now()
	s.reports[report.NodeID] = report
}

func (s *LoadStore) Get(nodeID string) (*LoadReport, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.reports[nodeID]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (s *LoadStore) Delete(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reports, nodeID)
}

func (s *LoadStore) List() map[string]*LoadReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*LoadReport, len(s.reports))
	for k, v := range s.reports {
		cp := *v
		result[k] = &cp
	}
	return result
}
