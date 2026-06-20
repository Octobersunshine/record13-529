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
	"task-scheduler/monitor"
	"task-scheduler/scheduler"
)

func main() {
	nodeStore := model.NewNodeStore()
	taskStore := model.NewTaskStore()
	loadStore := model.NewLoadStore()
	wrr := scheduler.NewWeightedRoundRobin()
	rebalancer := scheduler.NewRebalancer(nodeStore, taskStore, wrr)
	monCfg := monitor.DefaultConfig()
	loadMonitor := monitor.NewLoadMonitor(monCfg, loadStore, nodeStore, wrr, rebalancer)
	cronMgr := cron.NewCronManager(taskStore, wrr, nodeStore)

	go loadMonitor.Start()
	go cronMgr.Start()

	mux := http.NewServeMux()
	handler := api.NewHandler(nodeStore, taskStore, wrr, rebalancer, loadMonitor)
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
	fmt.Println("    POST   /api/rebalance      - Rebalance tasks across nodes")
	fmt.Println("    POST   /api/nodes/{id}/load - Report node load (cpu/mem)")
	fmt.Println("    GET    /api/stats          - View distribution statistics")
	fmt.Println()
	fmt.Println("  Auto-Rebalance Triggers:")
	fmt.Println("    - Node weight change     -> rebalance PENDING tasks")
	fmt.Println("    - Node status change     -> rebalance ALL active tasks")
	fmt.Println("    - Node deletion          -> migrate all tasks before removal")
	fmt.Println("    - CPU/Mem load change    -> auto-adjust effective weight + rebalance")
	fmt.Println()
	fmt.Println("  Dynamic Weight (CPU-Aware Scheduling):")
	fmt.Println("    Nodes report CPU/memory load via POST /api/nodes/{id}/load")
	fmt.Println("    EffectiveWeight = ConfiguredWeight * cpuFactor * memFactor")
	fmt.Println("    - CPU < 80%%:  gradual 0-20%% reduction")
	fmt.Println("    - CPU 80-95%%:  aggressive 50-100%% reduction")
	fmt.Println("    - CPU >= 95%%: critical, weight drops to minimum")
	fmt.Println("    EMA smoothing (alpha=0.3) prevents thrashing")
	fmt.Println("    Stale reports >60s -> revert to configured weight")
	fmt.Println("============================================")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		loadMonitor.Stop()
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
