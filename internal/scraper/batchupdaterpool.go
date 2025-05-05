package scraper

import (
	"context"
	"database/sql"
	"sync"

	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	investapi "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type batch struct {
	instrumentId string
	candles      []*investapi.HistoricCandle
}

type BatchUpdaterPool struct {
	stmt *sql.Stmt
	chs  []chan batch

	logger logger.Logger
}

func NewBatchUpdaterPool(stmt *sql.Stmt, poolSize, chSize int, logger logger.Logger) *BatchUpdaterPool {
	chs := make([]chan batch, poolSize)
	for i := range chs {
		chs[i] = make(chan batch, chSize)
	}

	return &BatchUpdaterPool{
		stmt:   stmt,
		chs:    chs,
		logger: logger,
	}
}

func (b *BatchUpdaterPool) Run(ctx context.Context, mainCh <-chan batch) {
	var wg sync.WaitGroup
	for i := range b.chs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			newSingleBatchUpdater(b.stmt, b.chs[i], b.logger).run(ctx)
		}()
	}
	defer func() {
		for i := range b.chs {
			close(b.chs[i])
		}
		wg.Wait()
	}()

	getPartitionFunc := roundRobinPartitionFunc(len(b.chs))

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-mainCh:
			if !ok {
				return
			}

			b.chs[getPartitionFunc()] <- msg
		}
	}
}

func roundRobinPartitionFunc(partitionsN int) func() int {
	n := 0
	return func() int {
		n++
		return n % partitionsN
	}
}
