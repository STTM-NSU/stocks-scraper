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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pgConfig := postgres.NewConfigFromEnv().Setup()
	zapLogger.Debugf("trying to connect to db with: %s", pgConfig)
	db, err := postgres.NewDB(pgConfig)
	if err != nil {
		zapLogger.Fatalf("%s: can't connect to db", err)
	}

	stmt, err := postgres.PrepareStmt(ctx, db)
	if err != nil {
		zapLogger.Fatalf("%s: can' prepare stmt", err)
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

	if scraperCfg.UseMOEXIndexInstruments {
		instrumentsId, err := instruments.GetMOEXIndexInstruments(investClient)
		if err != nil {
			zapLogger.Fatalf("%s: can't get moex index instruments", err)
		}
		scraperCfg.InstrumentsId = instrumentsId
	}

	daemonScraper := scraper.NewDaemonScraper(investClient, scraperCfg, _stocksChannelCap, zapLogger)
	h := handler.NewHandler(daemonScraper, zapLogger)
	batchUpdater := scraper.NewBatchUpdaterPool(stmt, _batchUpdaterPoolSize, _batchChannelCap, zapLogger)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		batchUpdater.Run(ctx, daemonScraper.GetStocksChan())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		daemonScraper.Run(ctx, h.GetCmdCh())
	}()

	srv := server.NewHTTPServer(scraperCfg.Port, h.InitRoutes())

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(); !errors.Is(err, http.ErrServerClosed) {
			zapLogger.Fatalf("can't start server: %s", err)
		}
	}()
	zapLogger.Infof("started server on port %s", scraperCfg.Port)

	<-ctx.Done()
	zapLogger.Infoln("shutdown server")
	wg.Wait()

	if err := srv.Shutdown(ctx); err != nil {
		zapLogger.Errorf("can't shutdown server: %s", err)
	}

	if err := stmt.Close(); err != nil {
		zapLogger.Errorf("%s: can't close stmt", err)
	}

	if err := db.Close(); err != nil {
		zapLogger.Errorf("%s: can't close db", err)
	}
}
