package cron

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"task-scheduler/model"
	"task-scheduler/scheduler"
)

type Dispatcher interface {
	DispatchTask(task *model.Task)
}

type CronManager struct {
	mu        sync.Mutex
	taskStore *model.TaskStore
	scheduler *scheduler.WeightedRoundRobin
	nodeStore *model.NodeStore
	jobs      map[string]*Job
	stopCh    chan struct{}
	running   bool
}

type Job struct {
	TaskID   string
	CronExpr string
	Spec     CronSpec
	NextRun  time.Time
	stopCh   chan struct{}
}

type CronSpec struct {
	Minute     []int
	Hour       []int
	DayOfMonth []int
	Month      []int
	DayOfWeek  []int
}

func NewCronManager(ts *model.TaskStore, s *scheduler.WeightedRoundRobin, ns *model.NodeStore) *CronManager {
	return &CronManager{
		taskStore: ts,
		scheduler: s,
		nodeStore: ns,
		jobs:      make(map[string]*Job),
		stopCh:    make(chan struct{}),
	}
}

func (cm *CronManager) Start() {
	cm.mu.Lock()
	if cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = true
	cm.mu.Unlock()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	now := time.Now()
	nextMinute := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, now.Location())
	time.Sleep(time.Until(nextMinute))

	for {
		select {
		case <-cm.stopCh:
			return
		case t := <-ticker.C:
			cm.checkAndFire(t.Truncate(time.Minute))
		}
	}
}

func (cm *CronManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.running {
		close(cm.stopCh)
		cm.running = false
	}
}

func (cm *CronManager) AddJob(taskID string, cronExpr string) error {
	spec, err := ParseCron(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	job := &Job{
		TaskID:   taskID,
		CronExpr: cronExpr,
		Spec:     spec,
		NextRun:  nextRunTime(spec, time.Now()),
	}
	cm.jobs[taskID] = job
	return nil
}

func (cm *CronManager) RemoveJob(taskID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.jobs, taskID)
}

func (cm *CronManager) ReloadJobs() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tasks := cm.taskStore.CronTasks()
	activeIDs := make(map[string]bool)

	for _, t := range tasks {
		activeIDs[t.ID] = true
		if _, exists := cm.jobs[t.ID]; !exists {
			spec, err := ParseCron(t.CronExpr)
			if err != nil {
				continue
			}
			cm.jobs[t.ID] = &Job{
				TaskID:   t.ID,
				CronExpr: t.CronExpr,
				Spec:     spec,
				NextRun:  nextRunTime(spec, time.Now()),
			}
		}
	}

	for id := range cm.jobs {
		if !activeIDs[id] {
			delete(cm.jobs, id)
		}
	}
}

func (cm *CronManager) ListJobs() []map[string]interface{} {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	result := make([]map[string]interface{}, 0, len(cm.jobs))
	for _, job := range cm.jobs {
		result = append(result, map[string]interface{}{
			"task_id":   job.TaskID,
			"cron_expr": job.CronExpr,
			"next_run":  job.NextRun.Format(time.RFC3339),
		})
	}
	return result
}

func (cm *CronManager) checkAndFire(t time.Time) {
	cm.mu.Lock()
	jobs := make([]*Job, 0)
	for _, job := range cm.jobs {
		jobs = append(jobs, job)
	}
	cm.mu.Unlock()

	for _, job := range jobs {
		if matchesCron(job.Spec, t) {
			task, ok := cm.taskStore.Get(job.TaskID)
			if !ok {
				cm.RemoveJob(job.TaskID)
				continue
			}

			node, err := cm.scheduler.Next()
			if err != nil {
				continue
			}

			cm.taskStore.Update(task.ID, func(t *model.Task) {
				t.Status = model.TaskStatusRunning
				t.AssignedNode = node.ID
			})
			cm.nodeStore.IncrementTaskCount(node.ID)

			go func(taskID, nodeID string) {
				time.Sleep(2 * time.Second)
				cm.taskStore.Update(taskID, func(t *model.Task) {
					t.Status = model.TaskStatusCompleted
				})
				cm.nodeStore.DecrementTaskCount(nodeID)
			}(task.ID, node.ID)
		}

		cm.mu.Lock()
		if j, exists := cm.jobs[job.TaskID]; exists {
			j.NextRun = nextRunTime(j.Spec, t)
		}
		cm.mu.Unlock()
	}
}

func ParseCron(expr string) (CronSpec, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return CronSpec{}, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}

	minute, err := parseField(fields[0], 0, 59)
	if err != nil {
		return CronSpec{}, fmt.Errorf("minute: %w", err)
	}
	hour, err := parseField(fields[1], 0, 23)
	if err != nil {
		return CronSpec{}, fmt.Errorf("hour: %w", err)
	}
	dayOfMonth, err := parseField(fields[2], 1, 31)
	if err != nil {
		return CronSpec{}, fmt.Errorf("day of month: %w", err)
	}
	month, err := parseField(fields[3], 1, 12)
	if err != nil {
		return CronSpec{}, fmt.Errorf("month: %w", err)
	}
	dayOfWeek, err := parseField(fields[4], 0, 6)
	if err != nil {
		return CronSpec{}, fmt.Errorf("day of week: %w", err)
	}

	return CronSpec{
		Minute:     minute,
		Hour:       hour,
		DayOfMonth: dayOfMonth,
		Month:      month,
		DayOfWeek:  dayOfWeek,
	}, nil
}

func parseField(field string, min, max int) ([]int, error) {
	if field == "*" {
		return rangeList(min, max), nil
	}

	values := make([]int, 0)
	parts := strings.Split(field, ",")

	for _, part := range parts {
		if strings.Contains(part, "/") {
			slashParts := strings.Split(part, "/")
			if len(slashParts) != 2 {
				return nil, fmt.Errorf("invalid step expression: %s", part)
			}
			step, err := strconv.Atoi(slashParts[1])
			if err != nil || step <= 0 {
				return nil, fmt.Errorf("invalid step value: %s", slashParts[1])
			}

			var start, end int
			if slashParts[0] == "*" {
				start = min
				end = max
			} else if strings.Contains(slashParts[0], "-") {
				rangeParts := strings.Split(slashParts[0], "-")
				if len(rangeParts) != 2 {
					return nil, fmt.Errorf("invalid range: %s", slashParts[0])
				}
				start, _ = strconv.Atoi(rangeParts[0])
				end, _ = strconv.Atoi(rangeParts[1])
			} else {
				start, _ = strconv.Atoi(slashParts[0])
				end = max
			}

			for i := start; i <= end; i += step {
				values = append(values, i)
			}
		} else if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range: %s", part)
			}
			start, err := strconv.Atoi(rangeParts[0])
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return nil, err
			}
			for i := start; i <= end; i++ {
				values = append(values, i)
			}
		} else {
			v, err := strconv.Atoi(part)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
	}

	return dedup(values), nil
}

func rangeList(min, max int) []int {
	result := make([]int, max-min+1)
	for i := range result {
		result[i] = min + i
	}
	return result
}

func dedup(vals []int) []int {
	seen := make(map[int]bool)
	result := make([]int, 0)
	for _, v := range vals {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

func matchesCron(spec CronSpec, t time.Time) bool {
	return contains(spec.Minute, t.Minute()) &&
		contains(spec.Hour, t.Hour()) &&
		contains(spec.DayOfMonth, t.Day()) &&
		contains(spec.Month, int(t.Month())) &&
		contains(spec.DayOfWeek, int(t.Weekday()))
}

func contains(vals []int, v int) bool {
	for _, val := range vals {
		if val == v {
			return true
		}
	}
	return false
}

func nextRunTime(spec CronSpec, from time.Time) time.Time {
	t := from.Add(1 * time.Minute).Truncate(time.Minute)

	for i := 0; i < 366*24*60; i++ {
		if matchesCron(spec, t) {
			return t
		}
		t = t.Add(1 * time.Minute)
	}

	return t
}
