package instruments

import (
	"fmt"

	"github.com/STTM-NSU/stocks-scraper/internal/config"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
)

// GetMOEXIndexInstruments возвращает список FIGI инструментов индекса MOEX
func GetMOEXIndexInstruments(c *investgo.Client) ([]string, error) {
	instumentId := make([]string, 0, len(config.MOEXTickers))
	for _, ticker := range config.MOEXTickers {
		figi, err := GetFIGIByTickerClassCode(c, ticker, "TQBR")
		if err != nil {
			return nil, err
		}
		if figi != "" {
			instumentId = append(instumentId, figi)
		}
	}

	return instumentId, nil
}

func GetFIGIByTickerClassCode(c *investgo.Client, ticker, classCode string) (string, error) {
	instrumentService := c.NewInstrumentsServiceClient()
	resp, err := instrumentService.InstrumentByTicker(ticker, classCode)
	if err != nil {
		return "", fmt.Errorf("%w: can't get isntrument", err)
	}

	return resp.GetInstrument().GetFigi(), nil
}
