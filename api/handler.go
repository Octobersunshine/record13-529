package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"task-scheduler/model"
	"task-scheduler/scheduler"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type Handler struct {
	nodeStore  *model.NodeStore
	taskStore  *model.TaskStore
	scheduler  *scheduler.WeightedRoundRobin
	rebalancer *scheduler.Rebalancer
}

func NewHandler(ns *model.NodeStore, ts *model.TaskStore, s *scheduler.WeightedRoundRobin, rb *scheduler.Rebalancer) *Handler {
	return &Handler{
		nodeStore:  ns,
		taskStore:  ts,
		scheduler:  s,
		rebalancer: rb,
	}
}

func writeJSON(w http.ResponseWriter, code int, resp Response) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/nodes", h.handleNodes)
	mux.HandleFunc("/api/nodes/", h.handleNodeByID)
	mux.HandleFunc("/api/tasks", h.handleTasks)
	mux.HandleFunc("/api/tasks/", h.handleTaskByID)
	mux.HandleFunc("/api/dispatch", h.handleDispatch)
	mux.HandleFunc("/api/rebalance", h.handleRebalance)
	mux.HandleFunc("/api/stats", h.handleStats)
}

func (h *Handler) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		nodes := h.nodeStore.List()
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: nodes})
	case http.MethodPost:
		var req struct {
			Name    string `json:"name"`
			Address string `json:"address"`
			Weight  int    `json:"weight"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "invalid request body"})
			return
		}
		if req.Name == "" || req.Address == "" {
			writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "name and address are required"})
			return
		}
		if req.Weight <= 0 {
			req.Weight = 1
		}
		now := time.Now()
		node := &model.Node{
			ID:        fmt.Sprintf("node-%d", now.UnixNano()),
			Name:      req.Name,
			Address:   req.Address,
			Weight:    req.Weight,
			Status:    model.NodeStatusOnline,
			CreatedAt: now,
			UpdatedAt: now,
		}
		h.nodeStore.Add(node)
		h.scheduler.AddNode(node)
		writeJSON(w, http.StatusCreated, Response{Code: 0, Message: "node created", Data: node})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
	}
}

func (h *Handler) handleNodeByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/nodes/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "node id is required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		node, ok := h.nodeStore.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, Response{Code: 404, Message: "node not found"})
			return
		}
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: node})

	case http.MethodPut:
		var req struct {
			Name   string `json:"name,omitempty"`
			Weight *int   `json:"weight,omitempty"`
			Status string `json:"status,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "invalid request body"})
			return
		}
		needRebalance := false
		var rebalanceIncludeRunning bool
		node, ok := h.nodeStore.Update(id, func(n *model.Node) {
			if req.Name != "" {
				n.Name = req.Name
			}
			if req.Weight != nil {
				n.Weight = *req.Weight
				h.scheduler.UpdateWeight(id, *req.Weight)
				needRebalance = true
				rebalanceIncludeRunning = false
			}
			if req.Status != "" {
				n.Status = model.NodeStatus(req.Status)
				h.scheduler.Sync(h.nodeStore.OnlineNodes())
				needRebalance = true
				rebalanceIncludeRunning = true
			}
		})
		if !ok {
			writeJSON(w, http.StatusNotFound, Response{Code: 404, Message: "node not found"})
			return
		}
		if needRebalance {
			go h.rebalancer.Rebalance(rebalanceIncludeRunning)
		}
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "node updated", Data: node})

	case http.MethodDelete:
		if _, ok := h.nodeStore.Get(id); !ok {
			writeJSON(w, http.StatusNotFound, Response{Code: 404, Message: "node not found"})
			return
		}
		h.rebalancer.RebalanceNode(id, true)
		h.nodeStore.Delete(id)
		h.scheduler.RemoveNode(id)
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "node deleted"})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
	}
}

func (h *Handler) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		tasks := h.taskStore.List()
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: tasks})
	case http.MethodPost:
		var req struct {
			Name     string `json:"name"`
			CronExpr string `json:"cron_expr,omitempty"`
			Payload  string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "invalid request body"})
			return
		}
		if req.Name == "" {
			writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "task name is required"})
			return
		}
		now := time.Now()
		task := &model.Task{
			ID:        fmt.Sprintf("task-%d", now.UnixNano()),
			Name:      req.Name,
			CronExpr:  req.CronExpr,
			Payload:   req.Payload,
			Status:    model.TaskStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		}
		h.taskStore.Add(task)

		if req.CronExpr == "" {
			h.dispatchTask(task)
		}

		writeJSON(w, http.StatusCreated, Response{Code: 0, Message: "task created", Data: task})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
	}
}

func (h *Handler) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, Response{Code: 400, Message: "task id is required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		task, ok := h.taskStore.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, Response{Code: 404, Message: "task not found"})
			return
		}
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: task})

	case http.MethodDelete:
		task, ok := h.taskStore.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, Response{Code: 404, Message: "task not found"})
			return
		}
		if task.AssignedNode != "" {
			h.nodeStore.DecrementTaskCount(task.AssignedNode)
		}
		h.taskStore.Delete(id)
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "task deleted"})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
	}
}

func (h *Handler) handleDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
		return
	}

	pending := h.taskStore.PendingTasks()
	if len(pending) == 0 {
		writeJSON(w, http.StatusOK, Response{Code: 0, Message: "no pending tasks to dispatch"})
		return
	}

	dispatched := make([]*model.Task, 0)
	for _, t := range pending {
		h.dispatchTask(t)
		updated, _ := h.taskStore.Get(t.ID)
		dispatched = append(dispatched, updated)
	}

	writeJSON(w, http.StatusOK, Response{Code: 0, Message: fmt.Sprintf("dispatched %d tasks", len(dispatched)), Data: dispatched})
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
		return
	}

	nodes := h.nodeStore.List()
	tasks := h.taskStore.List()

	taskStatusCount := make(map[string]int)
	for _, t := range tasks {
		taskStatusCount[string(t.Status)]++
	}

	nodeTaskMap := make(map[string]interface{})
	for _, n := range nodes {
		nodeTaskMap[n.ID] = map[string]interface{}{
			"name":       n.Name,
			"weight":     n.Weight,
			"status":     n.Status,
			"task_count": n.TaskCount,
		}
	}

	plan := h.rebalancer.ComputePlan()
	overloaded := make([]map[string]interface{}, 0)
	for _, o := range plan.Overloaded {
		overloaded = append(overloaded, map[string]interface{}{
			"node_id":        o.NodeID,
			"name":           o.Name,
			"weight":         o.Weight,
			"task_count":     o.TaskCount,
			"expected_tasks": o.ExpectedTasks,
			"delta":          o.Delta,
		})
	}
	underloaded := make([]map[string]interface{}, 0)
	for _, u := range plan.Underloaded {
		underloaded = append(underloaded, map[string]interface{}{
			"node_id":        u.NodeID,
			"name":           u.Name,
			"weight":         u.Weight,
			"task_count":     u.TaskCount,
			"expected_tasks": u.ExpectedTasks,
			"delta":          u.Delta,
		})
	}

	stats := map[string]interface{}{
		"total_nodes":      len(nodes),
		"online_nodes":     len(h.nodeStore.OnlineNodes()),
		"total_tasks":      len(tasks),
		"task_by_status":   taskStatusCount,
		"node_allocation":  nodeTaskMap,
		"weight_state":     h.scheduler.Stats(),
		"rebalance_needed": plan.TasksToMigrate,
		"overloaded_nodes": overloaded,
		"underloaded_nodes": underloaded,
	}

	writeJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: stats})
}

func (h *Handler) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Code: 405, Message: "method not allowed"})
		return
	}

	var req struct {
		IncludeRunning bool `json:"include_running,omitempty"`
	}
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	plan := h.rebalancer.Rebalance(req.IncludeRunning)

	result := map[string]interface{}{
		"migrated_tasks": plan.TasksToMigrate,
		"after":          h.rebalancer.ComputePlan(),
	}
	writeJSON(w, http.StatusOK, Response{Code: 0, Message: "rebalance completed", Data: result})
}

func (h *Handler) dispatchTask(task *model.Task) {
	node, err := h.scheduler.Next()
	if err != nil {
		return
	}

	h.taskStore.Update(task.ID, func(t *model.Task) {
		t.Status = model.TaskStatusRunning
		t.AssignedNode = node.ID
	})

	h.nodeStore.IncrementTaskCount(node.ID)
}
