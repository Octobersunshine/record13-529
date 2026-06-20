package model

import (
	"sync"
	"time"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
)

type Task struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	CronExpr     string     `json:"cron_expr,omitempty"`
	Payload      string     `json:"payload"`
	Status       TaskStatus `json:"status"`
	AssignedNode string     `json:"assigned_node,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type TaskStore struct {
	mu    sync.RWMutex
	tasks map[string]*Task
	order []string
}

func NewTaskStore() *TaskStore {
	return &TaskStore{
		tasks: make(map[string]*Task),
		order: make([]string, 0),
	}
}

func (s *TaskStore) Add(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[task.ID]; !exists {
		s.order = append(s.order, task.ID)
	}
	s.tasks[task.ID] = task
}

func (s *TaskStore) Get(id string) (*Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}

func (s *TaskStore) Update(id string, fn func(*Task)) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	fn(t)
	t.UpdatedAt = time.Now()
	cp := *t
	return &cp, true
}

func (s *TaskStore) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return false
	}
	delete(s.tasks, id)
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return true
}

func (s *TaskStore) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0, len(s.order))
	for _, id := range s.order {
		cp := *s.tasks[id]
		result = append(result, &cp)
	}
	return result
}

func (s *TaskStore) PendingTasks() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0)
	for _, id := range s.order {
		t := s.tasks[id]
		if t.Status == TaskStatusPending {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result
}

func (s *TaskStore) CronTasks() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0)
	for _, id := range s.order {
		t := s.tasks[id]
		if t.CronExpr != "" {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result
}
