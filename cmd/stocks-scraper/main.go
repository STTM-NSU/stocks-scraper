package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	"github.com/STTM-NSU/stocks-scraper/internal/config"
	"github.com/STTM-NSU/stocks-scraper/internal/handler"
	"github.com/STTM-NSU/stocks-scraper/internal/instruments"
	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/postgres"
	"github.com/STTM-NSU/stocks-scraper/internal/scraper"
	"github.com/STTM-NSU/stocks-scraper/internal/server"
	"github.com/joho/godotenv"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
)

const (
	_investCfgFilePath  = "./configs/invest.yaml"
	_scraperCfgFilePath = "./configs/config.yaml"

	_stocksChannelCap     = 100
	_batchUpdaterPoolSize = 10
	_batchChannelCap      = 100
)

func main() {
	zapLogger, loggerSync, err := logger.NewZapLogger(logger.Debug)
	if err != nil {
		log.Fatalf("%s: can't init logger", err)
	}
	defer loggerSync()

	if err := godotenv.Load(); err != nil {
		zapLogger.Warnf("can't detect .env file")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pgConfig := postgres.NewConfigFromEnv().Setup()
	zapLogger.Debugf("trying to connect to db with: %s", pgConfig)
	db, err := postgres.NewDB(pgConfig)
	if err != nil {
		zapLogger.Fatalf("%s: can't connect to db", err)
	}

	stocksStmt, err := postgres.PrepareStocksStmt(ctx, db)
	if err != nil {
		zapLogger.Fatalf("%s: can't prepare stmt", err)
	}

	instrStmt, err := postgres.PrepareInstrumentsStmt(ctx, db)
	if err != nil {
		zapLogger.Fatalf("%s: can't prepare stmt", err)
	}

	investCfg, err := config.LoadInvestConfig(_investCfgFilePath)
	if err != nil {
		zapLogger.Fatalf("%s: can't load invest cfg", err)
	}

	scraperCfg, err := config.LoadScraperConfig(_scraperCfgFilePath)
	if err != nil {
		zapLogger.Fatalf("%s: can't load scraper cfg", err)
	}

	investClient, err := investgo.NewClient(ctx, investCfg, zapLogger)
	if err != nil {
		zapLogger.Fatalf("%s", err)
	}

	instrumentsService := instruments.NewInstrumentService(investClient, instrStmt, zapLogger)
	daemonScraper := scraper.NewDaemonScraper(
		investClient,
		instrumentsService,
		scraperCfg.UseMOEXIndexInstruments,
		_stocksChannelCap,
		zapLogger,
	)
	h := handler.NewHandler(daemonScraper, zapLogger)
	batchUpdater := scraper.NewBatchUpdaterPool(stocksStmt, _batchUpdaterPoolSize, _batchChannelCap, zapLogger)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		batchUpdater.Run(ctx, daemonScraper.GetStocksChan())
		zapLogger.Infoln("shutdowned batch updater")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		daemonScraper.Run(ctx, h.GetCmdCh())
		zapLogger.Infoln("shutdowned daemon scraper")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := server.NewHTTPServer(ctx, scraperCfg.Port, h.InitRoutes())
		if err := srv.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zapLogger.Fatalf("can't start server: %s", err)
		}
		zapLogger.Infoln("server stopped")
	}()
	zapLogger.Infof("started server on port %s", scraperCfg.Port)

	<-ctx.Done()
	zapLogger.Infoln("start graceful shutdown")
	wg.Wait()

	if err := stocksStmt.Close(); err != nil {
		zapLogger.Errorf("%s: can't close stmt", err)
	} else {
		zapLogger.Infoln("shutdowned stocks stmt")
	}

	if err := instrStmt.Close(); err != nil {
		zapLogger.Errorf("%s: can't close stmt", err)
	} else {
		zapLogger.Infoln("shutdowned instr stmt")
	}

	if err := db.Close(); err != nil {
		zapLogger.Errorf("%s: can't close db", err)
	} else {
		zapLogger.Infoln("shutdowned db")
	}
}
