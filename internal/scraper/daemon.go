package scraper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/STTM-NSU/stocks-scraper/internal/config"
	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/model"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	investapi "github.com/russianinvestments/invest-api-go-sdk/proto"
)

const (
	_hourCandlesMaxDuration = 7 * 24 * time.Hour
	_waitInterval           = 4 * time.Hour
)

type instrData struct {
	cancelFunc context.CancelFunc
	ready      bool
}

type DaemonScraper struct {
	client   *investgo.Client
	cfg      config.ScraperConfig
	stocksCh chan batch
	wg       sync.WaitGroup

	instrData map[string]*instrData
	mu        sync.RWMutex

	logger logger.Logger
}

func NewDaemonScraper(client *investgo.Client,
	cfg config.ScraperConfig,
	stocksChanCap int,
	logger logger.Logger,
) *DaemonScraper {
	if stocksChanCap <= 0 {
		stocksChanCap = 5
	}

	return &DaemonScraper{
		client:    client,
		cfg:       cfg,
		stocksCh:  make(chan batch, stocksChanCap),
		instrData: make(map[string]*instrData),
		logger:    logger,
	}
}

func (s *DaemonScraper) GetStocksChan() <-chan batch {
	return s.stocksCh
}

func (s *DaemonScraper) Run(ctx context.Context, cmdCh <-chan model.UpdateMsg) {
	defer func() {
		s.wg.Wait()
		close(s.stocksCh)
	}()

	for _, instrId := range s.cfg.InstrumentsId {
		s.RunForSingleInstrument(ctx, instrId)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case cmd, ok := <-cmdCh:
			if !ok {
				return
			}
			switch cmd.Command {
			case model.Add:
				if cmd.InstrumentId != "" {
					s.RunForSingleInstrument(ctx, cmd.InstrumentId)
				}
			case model.Delete:
				s.mu.Lock()
				if i, ok := s.instrData[cmd.InstrumentId]; ok && i != nil {
					i.cancelFunc()
					delete(s.instrData, cmd.InstrumentId)
				}
				s.mu.Unlock()
			}
		}
	}
}

func (s *DaemonScraper) RunForSingleInstrument(ctx context.Context, instrId string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		s.mu.Lock()
		s.instrData[instrId] = &instrData{
			cancelFunc: cancel,
			ready:      false,
		}
		s.mu.Unlock()

		if err := s.consumeForSingleInstrument(ctx, instrId); err != nil {
			s.logger.Errorf("%s: can't consume for %s instrument", err, instrId)
		}
	}()
}

func (s *DaemonScraper) consumeForSingleInstrument(ctx context.Context, instrId string) error {
	mdClient := s.client.NewMarketDataServiceClient()
	getIntervalFunc := s.getNextInterval(_hourCandlesMaxDuration)

	for {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: context done for %s", ctx.Err(), instrId)
		}

		start, end := getIntervalFunc()
		resp, err := mdClient.GetCandles(instrId, investapi.CandleInterval_CANDLE_INTERVAL_HOUR, start, end, 0, 0)
		if err != nil {
			s.logger.Warnf("%s: can't get candlers for instrument %s and interval [%s, %s]", err, instrId, start, end)
			continue
		}
		s.stocksCh <- batch{
			instrumentId: instrId,
			candles:      resp.GetCandles(),
		}

		if end.After(time.Now()) {
			s.changeReady(instrId, true)
			s.logger.Infof("end scraping %s stocks for interval = [%s, %s, %s]", instrId, s.cfg.From, start, end)
			select {
			case <-time.After(_waitInterval):
				s.changeReady(instrId, false)
			case <-ctx.Done():
				return fmt.Errorf("%w: context done for %s", ctx.Err(), instrId)
			}
		}
	}
}

func (s *DaemonScraper) changeReady(instrumentId string, ready bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.instrData[instrumentId]; ok {
		s.instrData[instrumentId].ready = ready
	}
}

func (s *DaemonScraper) IsInstrumentReady(instrumentId string) (ok, exist bool) {
	s.mu.RLock()
	defer s.mu.Unlock()
	data, ok := s.instrData[instrumentId]
	if !ok || data == nil {
		return false, false
	}

	return data.ready, true
}

func (s *DaemonScraper) getNextInterval(limit time.Duration) func() (time.Time, time.Time) {
	start := s.cfg.From
	return func() (time.Time, time.Time) {
		next := start.Add(limit)
		now := time.Now()
		if next.After(now) {
			next = now
		}
		defer func() {
			start = next
		}()

		return start, next
	}
}
