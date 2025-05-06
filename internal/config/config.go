package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	"gopkg.in/yaml.v3"
)

var MOEXTickers = []string{
	"AFKS", "AFLT", "ALRS", "ASTR", "BSPB", "CBOM", "CHMF", "ENPG", "FEES", "GAZP",
	"GMKN", "HYDR", "IRAO", "LKOH", "MAGN", "MDMG", "MOEX", "MTLR", "MTLRP", "MTSS",
	"NLMK", "PHOR", "PIKK", "PLZL", "RNFT", "ROSN", "RTKM", "RTKMP", "RUAL", "SBER",
	"SBERP", "SELGP", "SNGS", "SNGSP", "SVCB", "T", "TATN", "TATNP", "TRNFP", "UPRO",
	"URKA", "VKCO", "VTBR", "YNDX",
}

type ScraperConfig struct {
	InstrumentsId           []string  `yaml:"instruments_id"`
	UseMOEXIndexInstruments bool      `yaml:"use_moex_index_instruments"`
	From                    time.Time `yaml:"from"`
	Port                    string    `yaml:"port"`
}

func LoadScraperConfig(filename string) (ScraperConfig, error) {
	var cfg ScraperConfig
	input, err := os.ReadFile(filename)
	if err != nil {
		return cfg, fmt.Errorf("%w: can't read file", err)
	}

	if err := yaml.Unmarshal(input, &cfg); err != nil {
		return cfg, fmt.Errorf("%w: can't unmarshal config", err)
	}

	if len(cfg.InstrumentsId) == 0 && !cfg.UseMOEXIndexInstruments {
		return cfg, fmt.Errorf("empty instruments ids")
	}

	if cfg.From.After(time.Now()) {
		return cfg, fmt.Errorf("from=%s can't be greater than now=%s", cfg.From, time.Now())
	}

	if cfg.Port == "" {
		return cfg, fmt.Errorf("empty port")
	}

	if _, err := strconv.Atoi(cfg.Port); err != nil {
		return cfg, fmt.Errorf("%w: port must be a number", err)
	}

	return cfg, nil
}

func LoadInvestConfig(filename string) (investgo.Config, error) {
	cfg, err := investgo.LoadConfig(filename)
	if err != nil {
		return investgo.Config{}, fmt.Errorf("%w: can't load config", err)
	}

	cfg.Token = os.Getenv("T_INVEST_API_TOKEN")
	if cfg.Token == "" {
		return investgo.Config{}, fmt.Errorf("empty t-invest api token")
	}

	return cfg, nil
}
