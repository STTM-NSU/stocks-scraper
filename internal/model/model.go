package model

import "time"

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
