package handler

import (
	"net/http"

	"github.com/STTM-NSU/stocks-scraper/internal/logger"
	"github.com/STTM-NSU/stocks-scraper/internal/model"
	"github.com/STTM-NSU/stocks-scraper/internal/scraper"
	"github.com/gin-gonic/gin"
)

const (
	_cmdChCap = 10
)

type Handler struct {
	daemonScraper *scraper.DaemonScraper
	cmdCh         chan model.UpdateMsg

	logger logger.Logger
}

func NewHandler(daemonScraper *scraper.DaemonScraper, logger logger.Logger) *Handler {
	return &Handler{
		daemonScraper: daemonScraper,
		cmdCh:         make(chan model.UpdateMsg, _cmdChCap),
		logger:        logger,
	}
}

func (h *Handler) GetCmdCh() <-chan model.UpdateMsg {
	return h.cmdCh
}

func (h *Handler) InitRoutes() http.Handler {
	r := gin.New()

	r.GET("/check", h.CheckStocksReadiness)
	r.POST("/scrap", h.ScrapForInstrument)

	return r
}

const (
	_instrumentIdQuery = "instrument_id"
	_cmdQuery          = "cmd"
)

func (h *Handler) CheckStocksReadiness(ctx *gin.Context) {
	instrId := ctx.Query(_instrumentIdQuery)
	if instrId == "" {
		ctx.String(http.StatusBadRequest, "empty instrument")
		return
	}

	ok, exist := h.daemonScraper.IsInstrumentReady(instrId)

	ctx.JSON(http.StatusOK, gin.H{
		"ok":    ok,
		"exist": exist,
	})
}

func (h *Handler) ScrapForInstrument(ctx *gin.Context) {
	instrId := ctx.Query(_instrumentIdQuery)
	if instrId == "" {
		ctx.String(http.StatusBadRequest, "empty instrument")
		return
	}

	cmd := ctx.Query(_cmdQuery)
	switch model.Command(cmd) {
	case model.Add, model.Delete:
		h.cmdCh <- model.UpdateMsg{
			Command:      model.Command(cmd),
			InstrumentId: instrId,
		}
	default:
		ctx.String(http.StatusBadRequest, "unknown cmd")
		return
	}

	ctx.String(http.StatusOK, "")
}
