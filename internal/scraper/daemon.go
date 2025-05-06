package scraper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/STTM-NSU/stocks-scraper/internal/instruments"
	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/model"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	investapi "github.com/russianinvestments/invest-api-go-sdk/proto"
	"go.uber.org/ratelimit"
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
	client             *investgo.Client
	from               time.Time
	instrumentsService *instruments.InstrumentService
	useMOEXInstruments bool

	stocksCh chan batch
	wg       sync.WaitGroup

	instrData map[string]*instrData
	mu        sync.RWMutex

	rateLimiter ratelimit.Limiter

	logger logger.Logger
}

func NewDaemonScraper(client *investgo.Client,
	instrumentsService *instruments.InstrumentService,
	useMOEXInstruments bool,
	stocksChanCap int,
	logger logger.Logger,
) *DaemonScraper {
	if stocksChanCap <= 0 {
		stocksChanCap = 5
	}

	return &DaemonScraper{
		client:             client,
		instrumentsService: instrumentsService,
		useMOEXInstruments: useMOEXInstruments,
		stocksCh:           make(chan batch, stocksChanCap),
		instrData:          make(map[string]*instrData),
		rateLimiter:        ratelimit.New(600, ratelimit.Per(1*time.Minute)),
		logger:             logger,
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

	var initInstruments []model.Instrument
	if s.useMOEXInstruments {
		if instrs, err := s.instrumentsService.GetMOEXIndexInstruments(); err != nil {
			s.logger.Errorf("%s: can't get MOEX instruments", err)
		} else {
			initInstruments = instrs
		}
	}

	for _, instr := range initInstruments {
		s.RunForSingleInstrument(ctx, instr)
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
					instr, err := s.instrumentsService.GetInstrumentById(cmd.InstrumentId)
					if err != nil {
						s.logger.Errorf("can't find instrument %s: %s", cmd.InstrumentId, err)
					}
					if instr != nil {
						s.RunForSingleInstrument(ctx, *instr)
					}
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

func (s *DaemonScraper) RunForSingleInstrument(ctx context.Context, instr model.Instrument) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		s.mu.Lock()
		s.instrData[instr.Id] = &instrData{
			cancelFunc: cancel,
			ready:      false,
		}
		s.mu.Unlock()

		if err := s.consumeForSingleInstrument(ctx, instr); err != nil {
			s.logger.Errorf("%s: can't consume for %s instrument", err, instr.Id)
		}
	}()
}

func (s *DaemonScraper) consumeForSingleInstrument(ctx context.Context, instr model.Instrument) error {
	mdClient := s.client.NewMarketDataServiceClient()
	getIntervalFunc := s.getNextInterval(_hourCandlesMaxDuration, instr.FirstCandleDate)

	s.logger.Infof("start consuming for %s [%s or %s]", instr.Id, s.from, instr.FirstCandleDate)

	for {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: context done for %s", ctx.Err(), instr.Id)
		}

		start, end := getIntervalFunc()

		s.rateLimiter.Take()
		resp, err := mdClient.GetCandles(instr.Id, investapi.CandleInterval_CANDLE_INTERVAL_HOUR, start, end, 0, 0)
		if err != nil {
			s.logger.Warnf("%s: can't get candles for instrument %s and interval [%s, %s]", err, instr.Id, start, end)
			continue
		}
		s.logger.Debugf("got candles [%s, %s, %s]: %v", instr.Id, start.Format(time.RFC3339), end.Format(time.RFC3339), resp.GetCandles())

		s.stocksCh <- batch{
			instrumentId: instr.Id,
			candles:      resp.GetCandles(),
		}

		if end.After(time.Now()) {
			s.changeReady(instr.Id, true)
			s.logger.Infof("end scraping %s stocks for interval = [%s, %s, %s]", instr.Id, s.from, start, end)
			select {
			case <-time.After(_waitInterval):
				s.changeReady(instr.Id, false)
			case <-ctx.Done():
				return fmt.Errorf("%w: context done for %s", ctx.Err(), instr.Id)
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
	defer s.mu.RUnlock()
	data, ok := s.instrData[instrumentId]
	if !ok || data == nil {
		return false, false
	}

	return data.ready, true
}

func (s *DaemonScraper) getNextInterval(limit time.Duration, firstCandleDate time.Time) func() (time.Time, time.Time) {
	start := s.from
	if start.Before(firstCandleDate) {
		start = firstCandleDate
	}
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
