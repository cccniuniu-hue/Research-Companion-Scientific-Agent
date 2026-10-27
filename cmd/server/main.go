// Research Companion Scientific Agent backend bootstrap.
// Validates secrets before infrastructure IO, assembles services and owns shutdown.
package main

import (
	"agi-assistant/config"
	authapp "agi-assistant/internal/application/auth"
	"agi-assistant/internal/application/chat"
	"agi-assistant/internal/application/memoryprojection"
	"agi-assistant/internal/application/memoryreconcile"
	skillapp "agi-assistant/internal/application/skill"
	authdomain "agi-assistant/internal/domain/auth"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/documentrepo"
	"agi-assistant/internal/infrastructure/persistence/longterm"
	"agi-assistant/internal/infrastructure/persistence/memoryoutbox"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"agi-assistant/internal/infrastructure/persistence/preference"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/skillrepo"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"agi-assistant/internal/infrastructure/persistence/userrepo"
	"agi-assistant/internal/infrastructure/platform/es"
	"agi-assistant/internal/infrastructure/platform/kafka"
	"agi-assistant/internal/infrastructure/platform/milvus"
	"agi-assistant/internal/infrastructure/platform/postgres"
	"agi-assistant/internal/infrastructure/projection/milvusmemory"
	"agi-assistant/internal/infrastructure/projection/neo4jmemory"
	"agi-assistant/internal/infrastructure/skillhub"
	"agi-assistant/internal/interfaces/http/handler"
	"agi-assistant/internal/pkg/logger"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// HTTP server 各档超时——所有 timeout 都是"不响应即断"，单位都给宽一点：
//   - ReadHeaderTimeout：客户端发送 header 的总时间。挡 Slowloris 攻击。
//   - ReadTimeout：含 body，POST 大对象（文档上传）也要在这内完成读取。
//   - WriteTimeout：从 header 读完到 response 写完。SSE 路由可能持续数分钟，
//     给 5 分钟兜底；超长流式建议改用 WebSocket 或在 handler 里 SetWriteDeadline(zero)。
//   - IdleTimeout：keep-alive 空闲连接最长保持时间，到点回收 goroutine。
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpWriteTimeout      = 5 * time.Minute
	httpIdleTimeout       = 120 * time.Second
	shutdownTimeout       = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("server failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.DefaultConfig()

	// 结构化日志：启动期用 text 格式便于本地看，生产改 json 需要读环境变量。
	// 后续 Phase 会把 format/level 挂到 config.yaml，这里先用工程常用默认。
	logger.Init(logger.Config{Format: "text", Level: "info", AddSource: false})

	if cfg.PprofEnabled && cfg.PprofAdminToken == "" {
		return fmt.Errorf("pprof enabled without admin token")
	}
	ttl := time.Duration(cfg.JWTTTLHours) * time.Hour
	issuer, err := authdomain.NewTokenIssuer(cfg.JWTSecret, ttl, cfg.JWTIssuer)
	if err != nil {
		return fmt.Errorf("JWT issuer configuration: %w", err)
	}

	// ── 平台层连接（每路独立失败降级，不阻塞启动）──
	logger.L().Info("bootstrap: connecting infrastructure")
	milvusClient, milvusStatus := milvus.Connect(cfg.MilvusConfig)
	pgDB, pgStatus := postgres.Connect(cfg.PostgresConfig)
	if pgDB != nil {
		postgres.BootstrapSchema(pgDB)
	}
	esClient, esStatus := es.Connect(cfg.ESConfig)
	kafkaWriter, kafkaStatus := kafka.Connect(cfg.KafkaConfig)
	defer func() {
		if milvusClient != nil {
			_ = milvusClient.Close()
		}
		if pgDB != nil {
			_ = pgDB.Close()
		}
		if kafkaWriter != nil {
			_ = kafkaWriter.Close()
		}
		logger.L().Info("bootstrap clients released")
	}()

	// ── Skill 广场服务（供 agent 主循环拉取已开启 skill + HTTP handler 使用）──
	var skillHub *skillhub.Client
	if cfg.SkillHubEnabled {
		skillHub = skillhub.NewClient(
			cfg.SkillHubGitHubToken, cfg.SkillHubKeyword,
			time.Duration(cfg.SkillHubCacheTTLMin)*time.Minute,
		)
	}
	skillSvc := skillapp.NewService(skillrepo.NewPGRepo(pgDB), skillHub, llm.New(cfg), cfg.SkillHubEnabled)

	// ── 仓储层（接口实现）──
	memoryRepo := memorytx.New(pgDB)
	outboxRepo := memoryoutbox.New(pgDB)
	deps := chat.Deps{
		ChatRepo:     chathistory.NewPGRepo(pgDB),
		PrefRepo:     preference.NewPGRepo(pgDB),
		SnapRepo:     snapshot.NewPGRepo(pgDB),
		LTMRepo:      longterm.NewPGRepo(pgDB),
		MemoryTxRepo: memoryRepo,
		RAGChunkRepo: ragchunk.NewStore(pgDB, milvusClient, esClient),
		DocumentRepo: documentrepo.NewStore(pgDB, ".data/documents"),
		Events:       eventbus.NewKafkaPublisher(kafkaWriter, kafkaStatus == "connected"),
		InfraStatus: map[string]string{
			"milvus":        milvusStatus,
			"pg":            pgStatus,
			"elasticsearch": esStatus,
			"kafka":         kafkaStatus,
		},
		EnabledSkillTools: skillSvc.EnabledTools,
	}

	// ── 初始化 UnifiedAgent + Auth Service + HTTP Server ──
	a := chat.New(cfg, deps)
	if kg := a.KG(); kg != nil {
		defer kg.Close()
	}

	// 补充「agent 内部初始化」的中间件状态到 banner / status：
	// Neo4j 知识图谱、命令沙箱、Skill 广场都是在 agent 内部装配的，
	// 这里仅补充启动横幅；Agent 的 infrastructure 是构造期独立快照。
	if kg := a.KG(); kg != nil && kg.Available() {
		deps.InfraStatus["neo4j"] = "connected"
	} else {
		deps.InfraStatus["neo4j"] = "disconnected"
	}
	if sb := a.Sandbox(); sb != nil {
		deps.InfraStatus["sandbox"] = sb.Backend()
	} else {
		deps.InfraStatus["sandbox"] = "disabled"
	}
	if cfg.SkillHubEnabled {
		deps.InfraStatus["skillhub"] = "enabled"
	} else {
		deps.InfraStatus["skillhub"] = "disabled"
	}

	authSvc := authapp.NewService(userrepo.NewPGRepo(pgDB), issuer)

	srv := newHTTPServer(cfg, a, authSvc, issuer, skillSvc)
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("HTTP listen: %w", err)
	}
	defer listener.Close()

	// ── 监听信号 + 启动 server ──
	// signal.NotifyContext 会在收到 SIGINT/SIGTERM 时 cancel ctx，
	// 主流程检测到 ctx.Done() 后开始优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	memoryCtx, stopMemory := context.WithCancel(context.Background())
	var memoryWG sync.WaitGroup
	defer func() { stopMemory(); memoryWG.Wait() }()
	runWorker := func(worker *memoryprojection.Worker) {
		memoryWG.Add(1)
		go func() {
			defer memoryWG.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-memoryCtx.Done():
					return
				case <-ticker.C:
					if err := worker.RunOnce(memoryCtx); err != nil {
						logger.L().Warn("memory projection worker cycle failed", "err", err)
					}
				}
			}
		}()
	}
	if milvusClient != nil {
		store := milvusmemory.NewMilvusStore(milvusClient, cfg.RAGMilvusDim)
		if err := store.Init(memoryCtx); err != nil {
			logger.L().Warn("memory milvus projection disabled", "err", err)
		} else {
			runWorker(memoryprojection.NewWorker(outboxRepo, milvusmemory.New(store),
				memoryprojection.Config{WorkerID: "milvus-memory", BatchSize: 100, Lease: 30 * time.Second, MaxAttempts: 10}))
			reconciler := memoryreconcile.New(memoryRepo, store, outboxRepo, "milvus", 500)
			memoryWG.Add(1)
			go runReconciler(memoryCtx, &memoryWG, reconciler, 6*time.Hour)
		}
	}
	if kg := a.KG(); kg != nil && kg.Available() {
		store := neo4jmemory.NewNeo4jStore(kg.Client())
		runWorker(memoryprojection.NewWorker(outboxRepo, neo4jmemory.New(store),
			memoryprojection.Config{WorkerID: "neo4j-memory", BatchSize: 100, Lease: 30 * time.Second, MaxAttempts: 10}))
		reconciler := memoryreconcile.New(memoryRepo, store, outboxRepo, "neo4j", 500)
		memoryWG.Add(1)
		go runReconciler(memoryCtx, &memoryWG, reconciler, 6*time.Hour)
	}

	// 服务器在独立 goroutine 跑，主 goroutine 等信号
	serverErr := make(chan error, 1)
	go func() {
		printBanner(cfg, deps.InfraStatus)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	// 等信号或 HTTP Serve 结果触发关停：
	//   1) 系统信号（ctx.Done）→ 正常关停
	//   2) Serve 异常返回 → 清理资源后报告错误
	var serveErr error
	select {
	case err := <-serverErr:
		if err != nil {
			serveErr = err
			logger.L().Error("HTTP server failed", "err", err)
		}
	case <-ctx.Done():
		logger.L().Info("shutdown signal received, starting graceful shutdown")
	}

	// ── Graceful Shutdown ──
	// 先关 HTTP server：拒绝新连接，等 in-flight 请求在 shutdownTimeout 内完成。
	// 超时会关闭连接；同步工具仍需响应取消，不能承诺回滚已发生的副作用。
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownHTTP(shutCtx, srv, a); err != nil {
		logger.L().Warn("http shutdown forced connection close", slog.Any("err", err))
		if serveErr == nil {
			serveErr = err
		}
	} else {
		logger.L().Info("http server closed")
	}
	return serveErr
}

func shutdownHTTP(ctx context.Context, srv *http.Server, a *chat.UnifiedAgent) error {
	if a != nil {
		a.Cancel()
	}
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()
		return err
	}
	return nil
}

func runReconciler(ctx context.Context, wg *sync.WaitGroup, reconciler *memoryreconcile.Reconciler, interval time.Duration) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if report, err := reconciler.RunOnce(ctx); err != nil {
				logger.L().Warn("memory reconciliation failed", "err", err)
			} else if report.RepairEnqueued > 0 {
				logger.L().Info("memory reconciliation enqueued repairs", "count", report.RepairEnqueued)
			}
		}
	}
}

// newHTTPServer 装配 chi.Router + 三大 timeout。
// 单独抽出便于测试（直接 srv.Handler.ServeHTTP）和未来加 TLS。
func newHTTPServer(cfg *config.APIConfig, a *chat.UnifiedAgent, authSvc *authapp.Service, issuer *authdomain.TokenIssuer, skills *skillapp.Service) *http.Server {
	h := handler.New(a, authSvc, issuer, cfg, skills)
	return &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           h.Handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func printBanner(cfg *config.APIConfig, status map[string]string) {
	addr := ":" + cfg.ServerPort
	fmt.Println("========================================")
	fmt.Println("Research Companion Scientific Agent API")
	fmt.Println("========================================")

	fmt.Printf("[INFO] Service       http://localhost%s\n", addr)
	fmt.Printf("[INFO] 通用模型           %s\n", cfg.LLMModel)
	fmt.Printf("[INFO] Embedding     %s\n", cfg.EmbeddingModel)

	fmt.Println("----------------------------------------")

	fmt.Printf("[INFO] Milvus        %s\n", status["milvus"])
	fmt.Printf("[INFO] PostgreSQL    %s:%d (%s)\n", cfg.PGHost, cfg.PGPort, status["pg"])
	fmt.Printf("[INFO] ElasticSearch %s\n", status["elasticsearch"])
	fmt.Printf("[INFO] Kafka         %s\n", status["kafka"])
	fmt.Printf("[INFO] Neo4j(KG)     %s\n", status["neo4j"])
	fmt.Printf("[INFO] Sandbox       %s\n", status["sandbox"])
	skillhub := status["skillhub"]
	if skillhub == "enabled" {
		skillhub = fmt.Sprintf("enabled (keyword: %s)", cfg.SkillHubKeyword)
	}
	fmt.Printf("[INFO] Skill广场      %s\n", skillhub)

	fmt.Println("----------------------------------------")
	fmt.Println("[READY] 道阻且长，行则将至。")
	fmt.Println("========================================")
}
