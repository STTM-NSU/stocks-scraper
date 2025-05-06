package scraper

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/model"
)

type singleBatchUpdater struct {
	stmt      *sql.Stmt
	batchChan <-chan batch
	logger    logger.Logger
}

func newSingleBatchUpdater(stmt *sql.Stmt, batchChan <-chan batch, logger logger.Logger) *singleBatchUpdater {
	return &singleBatchUpdater{
		stmt:      stmt,
		batchChan: batchChan,
		logger:    logger,
	}
}

func (s *singleBatchUpdater) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-s.batchChan:
			if !ok {
				return
			}
			for _, c := range v.candles {
				if err := s.saveStockDB(model.Stock{
					InstrumentId: v.instrumentId,
					ClosePrice:   c.GetClose().ToFloat(),
					Timestamp:    c.GetTime().AsTime(),
				}); err != nil {
					s.logger.Errorf("%s: can't save stocks to db for %s", err, v.instrumentId)
				}
			}
		}
	}
}

func (s *singleBatchUpdater) saveStockDB(stock model.Stock) error {
	_, err := s.stmt.Exec(stock.InstrumentId, stock.Timestamp, stock.ClosePrice)
	if err != nil {
		return fmt.Errorf("%w: can't exec stmt", err)
	}
	s.logger.Infof("save data to db: [%v]", stock)

	return nil
}
