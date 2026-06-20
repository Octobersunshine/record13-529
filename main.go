package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"task-scheduler/api"
	"task-scheduler/cron"
	"task-scheduler/model"
	"task-scheduler/scheduler"
)

func main() {
	nodeStore := model.NewNodeStore()
	taskStore := model.NewTaskStore()
	wrr := scheduler.NewWeightedRoundRobin()
	cronMgr := cron.NewCronManager(taskStore, wrr, nodeStore)

	go cronMgr.Start()

	mux := http.NewServeMux()
	handler := api.NewHandler(nodeStore, taskStore, wrr)
	handler.RegisterRoutes(mux)

	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	fmt.Println("============================================")
	fmt.Println("  Task Scheduler - Weighted Round Robin")
	fmt.Println("============================================")
	fmt.Printf("  Server listening on %s\n", addr)
	fmt.Println()
	fmt.Println("  API Endpoints:")
	fmt.Println("    POST   /api/nodes          - Add execution node")
	fmt.Println("    GET    /api/nodes          - List all nodes")
	fmt.Println("    GET    /api/nodes/{id}     - Get node detail")
	fmt.Println("    PUT    /api/nodes/{id}     - Update node (name/weight/status)")
	fmt.Println("    DELETE /api/nodes/{id}     - Remove node")
	fmt.Println("    POST   /api/tasks          - Create task (with optional cron_expr)")
	fmt.Println("    GET    /api/tasks          - List all tasks")
	fmt.Println("    GET    /api/tasks/{id}     - Get task detail")
	fmt.Println("    DELETE /api/tasks/{id}     - Delete task")
	fmt.Println("    POST   /api/dispatch       - Manually dispatch pending tasks")
	fmt.Println("    GET    /api/stats          - View distribution statistics")
	fmt.Println()
	fmt.Println("  Weighted Round Robin Scheduling:")
	fmt.Println("    Each node has a configurable weight.")
	fmt.Println("    Tasks are distributed proportionally to node weights.")
	fmt.Println("    Example: node-A(weight=3), node-B(weight=1)")
	fmt.Println("    -> 75% tasks to A, 25% tasks to B")
	fmt.Println("============================================")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cronMgr.Stop()
		os.Exit(0)
	}()

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
