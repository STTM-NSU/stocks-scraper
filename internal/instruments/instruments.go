package instruments

import (
	"database/sql"
	"fmt"

	"github.com/STTM-NSU/stocks-scraper/internal/config"
	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/model"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
)

type InstrumentService struct {
	client *investgo.InstrumentsServiceClient
	stmt   *sql.Stmt
	logger logger.Logger
}

func NewInstrumentService(c *investgo.Client, stmt *sql.Stmt, logger logger.Logger) *InstrumentService {
	return &InstrumentService{
		client: c.NewInstrumentsServiceClient(),
		stmt:   stmt,
		logger: logger,
	}
}

// GetMOEXIndexInstruments возвращает список инструментов индекса MOEX
func (s *InstrumentService) GetMOEXIndexInstruments() ([]model.Instrument, error) {
	instruments := make([]model.Instrument, 0, len(config.MOEXTickers))
	for _, ticker := range config.MOEXTickers {
		instrument, err := s.GetInstrumentByTickerClassCode(ticker, "TQBR")
		if err != nil {
			return nil, err
		}
		if instrument != nil {
			instruments = append(instruments, *instrument)
		}
	}

	return instruments, nil
}

func (s *InstrumentService) GetInstrumentByTickerClassCode(ticker, classCode string) (*model.Instrument, error) {
	resp, err := s.client.InstrumentByTicker(ticker, classCode)
	if err != nil {
		return nil, fmt.Errorf("%w: can't get isntrument", err)
	}

	instr := &model.Instrument{
		Id:              resp.GetInstrument().GetFigi(),
		FirstCandleDate: resp.GetInstrument().GetFirst_1MinCandleDate().AsTime(),
	}

	if err := s.saveInstrumentToDB(instr); err != nil {
		s.logger.Warnf("%s: can't save instrument to db", err)
	}

	return instr, nil
}

func (s *InstrumentService) GetInstrumentById(id string) (*model.Instrument, error) {
	resp, err := s.client.InstrumentByFigi(id)
	if err != nil {
		return nil, fmt.Errorf("%w: can't get isntrument", err)
	}

	instr := &model.Instrument{
		Id:              resp.GetInstrument().GetFigi(),
		FirstCandleDate: resp.GetInstrument().GetFirst_1MinCandleDate().AsTime(),
	}

	if err := s.saveInstrumentToDB(instr); err != nil {
		s.logger.Warnf("%s: can't save instrument to db", err)
	}

	return instr, nil
}

func (s *InstrumentService) saveInstrumentToDB(i *model.Instrument) error {
	if i == nil {
		return fmt.Errorf("empty instrument")
	}

	_, err := s.stmt.Exec(i.Id, i.FirstCandleDate)
	if err != nil {
		return fmt.Errorf("%w: can't exec stmt", err)
	}
	s.logger.Debugf("save instrument to db: [%v]", i)

	return nil
}
