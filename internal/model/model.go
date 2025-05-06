package model

import "time"

type Instrument struct {
	Id              string
	FirstCandleDate time.Time // we retrieve first 1 min candle to operate hour candles
}

type Stock struct {
	InstrumentId string    `db:"instrument_id"`
	ClosePrice   float64   `db:"close_price"`
	Timestamp    time.Time `db:"ts"`
}

type Command string

const (
	Add    Command = "add"
	Delete Command = "delete"
)

type UpdateMsg struct {
	Command      Command
	InstrumentId string
}
