// Command syncd runs the sync server.
//
//	syncd -addr :8080 -db sync.db
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dakribe/sync-engine/server"
	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/store/sqlite"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dbPath := flag.String("db", "sync.db", "SQLite database file, or empty to keep data in memory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*addr, *dbPath, logger); err != nil {
		logger.Error("syncd stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, dbPath string, logger *slog.Logger) error {
	var st store.Store = memory.New()
	if dbPath != "" {
		db, err := sqlite.Open(dbPath)
		if err != nil {
			return err
		}
		st = db
	}
	defer st.Close()

	engine := server.NewEngine(st)
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.NewHandler(engine, logger),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// Shutdown waits for open requests, and event streams never end on their own.
	srv.RegisterOnShutdown(engine.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("syncd listening", "addr", addr, "db", dbPath)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
