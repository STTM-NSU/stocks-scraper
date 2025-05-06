package server

import (
	"context"
	"net"
	"net/http"
	"time"
)

type HTTPServer struct {
	s *http.Server
}

func NewHTTPServer(ctx context.Context, port string, handler http.Handler) *HTTPServer {
	return &HTTPServer{
		s: &http.Server{
			Handler:           handler,
			Addr:              ":" + port,
			ReadHeaderTimeout: 10 * time.Second,
			BaseContext: func(listener net.Listener) context.Context {
				return ctx
			},
		},
	}
}

func (s *HTTPServer) Start() error {
	return s.s.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}

func (s *HTTPServer) Run(ctx context.Context) error {
	errCh := make(chan error)
	go func() {
		errCh <- s.Start()
	}()
	select {
	case <-ctx.Done():
		return s.Shutdown(ctx)
	case err := <-errCh:
		return err
	}
}
