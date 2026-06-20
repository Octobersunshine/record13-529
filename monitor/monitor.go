package monitor

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"task-scheduler/model"
	"task-scheduler/scheduler"
)

type Config struct {
	CheckInterval       time.Duration
	CPUHighThreshold    float64
	CPUCriticalThreshold float64
	MinEffectiveWeight  int
	RebalanceDeltaPct   float64
	EMAlpha             float64
	StaleTimeout        time.Duration
}

func DefaultConfig() Config {
	return Config{
		CheckInterval:        10 * time.Second,
		CPUHighThreshold:    80.0,
		CPUCriticalThreshold: 95.0,
		MinEffectiveWeight:   1,
		RebalanceDeltaPct:    20.0,
		EMAlpha:              0.3,
		StaleTimeout:         60 * time.Second,
	}
}

type nodeState struct {
	emaCPU float64
	emaMem float64
}

type LoadMonitor struct {
	config     Config
	loadStore  *model.LoadStore
	nodeStore  *model.NodeStore
	wrr        *scheduler.WeightedRoundRobin
	rebalancer *scheduler.Rebalancer

	mu     sync.Mutex
	states map[string]*nodeState
	stopCh chan struct{}
	running bool
}

func NewLoadMonitor(cfg Config, ls *model.LoadStore, ns *model.NodeStore, wrr *scheduler.WeightedRoundRobin, rb *scheduler.Rebalancer) *LoadMonitor {
	return &LoadMonitor{
		config:     cfg,
		loadStore:  ls,
		nodeStore:  ns,
		wrr:        wrr,
		rebalancer: rb,
		states:     make(map[string]*nodeState),
		stopCh:     make(chan struct{}),
	}
}

func (m *LoadMonitor) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()

	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	m.recalcAll()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.recalcAll()
		}
	}
}

func (m *LoadMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		close(m.stopCh)
		m.running = false
	}
}

func (m *LoadMonitor) ReportLoad(report *model.LoadReport) {
	m.loadStore.Update(report)

	m.mu.Lock()
	st, exists := m.states[report.NodeID]
	if !exists {
		st = &nodeState{
			emaCPU: report.CPUUsage,
			emaMem: report.MemUsage,
		}
		m.states[report.NodeID] = st
	} else {
		a := m.config.EMAlpha
		st.emaCPU = a*report.CPUUsage + (1-a)*st.emaCPU
		st.emaMem = a*report.MemUsage + (1-a)*st.emaMem
	}
	m.mu.Unlock()

	m.recalcNode(report.NodeID)
}

func (m *LoadMonitor) GetNodeState(nodeID string) (emaCPU, emaMem float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.states[nodeID]
	if !ok {
		return 0, 0
	}
	return st.emaCPU, st.emaMem
}

func (m *LoadMonitor) recalcAll() {
	nodes := m.nodeStore.OnlineNodes()
	needRebalance := false

	for _, n := range nodes {
		if m.recalcNode(n.ID) {
			needRebalance = true
		}
	}

	if needRebalance {
		log.Println("[LoadMonitor] effective weight changed, triggering auto-rebalance")
		go m.rebalancer.Rebalance(true)
	}
}

func (m *LoadMonitor) recalcNode(nodeID string) bool {
	node, ok := m.nodeStore.Get(nodeID)
	if !ok || node.Status != model.NodeStatusOnline {
		return false
	}

	report, hasReport := m.loadStore.Get(nodeID)

	if !hasReport || time.Since(report.ReportedAt) > m.config.StaleTimeout {
		changed := false
		if node.EffectiveWeight != node.Weight {
			m.nodeStore.Update(nodeID, func(n *model.Node) {
				n.EffectiveWeight = n.Weight
				n.CPUUsage = 0
				n.MemUsage = 0
			})
			m.wrr.UpdateWeight(nodeID, node.Weight)
			changed = true
		}
		return changed
	}

	m.mu.Lock()
	st, exists := m.states[nodeID]
	if !exists {
		st = &nodeState{
			emaCPU: report.CPUUsage,
			emaMem: report.MemUsage,
		}
		m.states[nodeID] = st
	}
	emaCPU := st.emaCPU
	emaMem := st.emaMem
	m.mu.Unlock()

	newWeight := m.calculateEffectiveWeight(node.Weight, emaCPU, emaMem)

	changed := false
	oldEffective := node.EffectiveWeight
	if newWeight != oldEffective {
		deltaPct := 0.0
		if oldEffective > 0 {
			deltaPct = math.Abs(float64(newWeight-oldEffective)) / float64(oldEffective) * 100
		} else {
			deltaPct = 100.0
		}

		m.nodeStore.Update(nodeID, func(n *model.Node) {
			n.EffectiveWeight = newWeight
			n.CPUUsage = emaCPU
			n.MemUsage = emaMem
		})
		m.wrr.UpdateWeight(nodeID, newWeight)

		if deltaPct >= m.config.RebalanceDeltaPct {
			changed = true
		}

		log.Printf("[LoadMonitor] node=%s weight=%d effective=%d->%d cpu=%.1f%% mem=%.1f%% changed=%.1f%%",
			nodeID, node.Weight, oldEffective, newWeight, emaCPU, emaMem, deltaPct)
	}

	return changed
}

func (m *LoadMonitor) calculateEffectiveWeight(configuredWeight int, cpuUsage, memUsage float64) int {
	cpuFactor := m.cpuFactor(cpuUsage)
	memFactor := m.memFactor(memUsage)

	factor := cpuFactor * memFactor

	effective := float64(configuredWeight) * factor
	effective = math.Max(effective, float64(m.config.MinEffectiveWeight))
	effective = math.Round(effective)

	result := int(effective)
	if result < m.config.MinEffectiveWeight {
		result = m.config.MinEffectiveWeight
	}
	return result
}

func (m *LoadMonitor) cpuFactor(cpuUsage float64) float64 {
	if cpuUsage <= 0 {
		return 1.0
	}
	if cpuUsage >= m.config.CPUCriticalThreshold {
		return float64(m.config.MinEffectiveWeight) / float64(10)
	}
	if cpuUsage >= m.config.CPUHighThreshold {
		excess := cpuUsage - m.config.CPUHighThreshold
		range_ := m.config.CPUCriticalThreshold - m.config.CPUHighThreshold
		penalty := 0.5 * (excess / range_)
		return 1.0 - penalty
	}

	gradual := cpuUsage / m.config.CPUHighThreshold
	return 1.0 - 0.2*gradual
}

func (m *LoadMonitor) memFactor(memUsage float64) float64 {
	if memUsage <= 0 {
		return 1.0
	}
	if memUsage >= 95.0 {
		return 0.3
	}
	if memUsage >= 85.0 {
		excess := memUsage - 85.0
		return 1.0 - 0.7*(excess/10.0)
	}
	return 1.0 - 0.1*(memUsage/85.0)
}

func (m *LoadMonitor) RemoveNode(nodeID string) {
	m.loadStore.Delete(nodeID)
	m.mu.Lock()
	delete(m.states, nodeID)
	m.mu.Unlock()
}

func (m *LoadMonitor) Stats() map[string]interface{} {
	m.mu.Lock()
	stateCopy := make(map[string]interface{})
	for id, st := range m.states {
		stateCopy[id] = map[string]interface{}{
			"ema_cpu": fmt.Sprintf("%.1f%%", st.emaCPU),
			"ema_mem": fmt.Sprintf("%.1f%%", st.emaMem),
		}
	}
	m.mu.Unlock()

	reports := m.loadStore.List()
	reportCopy := make(map[string]interface{})
	for id, r := range reports {
		reportCopy[id] = map[string]interface{}{
			"cpu_usage":    fmt.Sprintf("%.1f%%", r.CPUUsage),
			"mem_usage":    fmt.Sprintf("%.1f%%", r.MemUsage),
			"load_avg_1":   r.LoadAvg1,
			"load_avg_5":   r.LoadAvg5,
			"load_avg_15":  r.LoadAvg15,
			"active_conns": r.ActiveConns,
			"reported_at":  r.ReportedAt.Format(time.RFC3339),
		}
	}

	return map[string]interface{}{
		"config": map[string]interface{}{
			"check_interval":         m.config.CheckInterval.String(),
			"cpu_high_threshold":     fmt.Sprintf("%.0f%%", m.config.CPUHighThreshold),
			"cpu_critical_threshold": fmt.Sprintf("%.0f%%", m.config.CPUCriticalThreshold),
			"min_effective_weight":   m.config.MinEffectiveWeight,
			"rebalance_delta_pct":    fmt.Sprintf("%.0f%%", m.config.RebalanceDeltaPct),
			"ema_alpha":              m.config.EMAlpha,
			"stale_timeout":          m.config.StaleTimeout.String(),
		},
		"ema_states":    stateCopy,
		"raw_reports":   reportCopy,
	}
}
