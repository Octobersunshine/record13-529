package scheduler

import (
	"math"

	"task-scheduler/model"
)

type RebalancePlan struct {
	TotalTasks     int
	Overloaded     []*NodeLoadInfo
	Underloaded    []*NodeLoadInfo
	TasksToMigrate int
}

type NodeLoadInfo struct {
	NodeID        string
	Name          string
	Weight        int
	TaskCount     int
	ExpectedTasks float64
	Delta         int
}

type Rebalancer struct {
	nodeStore *model.NodeStore
	taskStore *model.TaskStore
	wrr       *WeightedRoundRobin
}

func NewRebalancer(ns *model.NodeStore, ts *model.TaskStore, wrr *WeightedRoundRobin) *Rebalancer {
	return &Rebalancer{
		nodeStore: ns,
		taskStore: ts,
		wrr:       wrr,
	}
}

func (r *Rebalancer) ComputePlan() *RebalancePlan {
	nodes := r.nodeStore.OnlineNodes()
	if len(nodes) == 0 {
		return &RebalancePlan{}
	}

	totalWeight := 0
	totalTasks := 0
	for _, n := range nodes {
		w := n.EffectiveWeight
		if w <= 0 {
			w = n.Weight
		}
		totalWeight += w
		totalTasks += n.TaskCount
	}
	if totalWeight == 0 || totalTasks == 0 {
		return &RebalancePlan{}
	}

	plan := &RebalancePlan{
		TotalTasks:  totalTasks,
		Overloaded:  make([]*NodeLoadInfo, 0),
		Underloaded: make([]*NodeLoadInfo, 0),
	}

	base := float64(totalTasks) / float64(totalWeight)
	for _, n := range nodes {
		w := n.EffectiveWeight
		if w <= 0 {
			w = n.Weight
		}
		expected := base * float64(w)
		delta := n.TaskCount - int(math.Round(expected))
		info := &NodeLoadInfo{
			NodeID:        n.ID,
			Name:          n.Name,
			Weight:        w,
			TaskCount:     n.TaskCount,
			ExpectedTasks: expected,
			Delta:         delta,
		}
		if delta > 0 {
			plan.Overloaded = append(plan.Overloaded, info)
			plan.TasksToMigrate += delta
		} else if delta < 0 {
			plan.Underloaded = append(plan.Underloaded, info)
		}
	}

	return plan
}

func (r *Rebalancer) Rebalance(includeRunning bool) *RebalancePlan {
	plan := r.ComputePlan()
	if plan.TasksToMigrate == 0 {
		return plan
	}

	r.wrr.ResetCurrentWeights()

	migratedCount := 0
	for _, over := range plan.Overloaded {
		if over.Delta <= 0 {
			continue
		}

		candidates := r.taskStore.ActiveTasksByNode(over.NodeID)
		if !includeRunning {
			filtered := make([]*model.Task, 0)
			for _, t := range candidates {
				if t.Status == model.TaskStatusPending {
					filtered = append(filtered, t)
				}
			}
			candidates = filtered
		}

		migrateNum := over.Delta
		if len(candidates) < migrateNum {
			migrateNum = len(candidates)
		}

		for i := 0; i < migrateNum; i++ {
			task := candidates[i]

			r.taskStore.ResetTaskNode(task.ID)
			r.nodeStore.DecrementTaskCount(over.NodeID)

			newNode, err := r.wrr.Next()
			if err != nil {
				r.nodeStore.IncrementTaskCount(over.NodeID)
				continue
			}

			r.taskStore.Update(task.ID, func(t *model.Task) {
				t.Status = model.TaskStatusRunning
				t.AssignedNode = newNode.ID
			})
			r.nodeStore.IncrementTaskCount(newNode.ID)
			migratedCount++
		}
	}

	plan.TasksToMigrate = migratedCount
	return plan
}

func (r *Rebalancer) RebalanceNode(nodeID string, includeRunning bool) int {
	r.wrr.ResetCurrentWeights()

	tasks := r.taskStore.ActiveTasksByNode(nodeID)
	if !includeRunning {
		filtered := make([]*model.Task, 0)
		for _, t := range tasks {
			if t.Status == model.TaskStatusPending {
				filtered = append(filtered, t)
			}
		}
		tasks = filtered
	}

	migratedCount := 0
	for _, task := range tasks {
		r.taskStore.ResetTaskNode(task.ID)
		r.nodeStore.DecrementTaskCount(nodeID)

		newNode, err := r.wrr.Next()
		if err != nil {
			r.nodeStore.IncrementTaskCount(nodeID)
			continue
		}

		r.taskStore.Update(task.ID, func(t *model.Task) {
			t.Status = model.TaskStatusRunning
			t.AssignedNode = newNode.ID
		})
		r.nodeStore.IncrementTaskCount(newNode.ID)
		migratedCount++
	}

	return migratedCount
}
