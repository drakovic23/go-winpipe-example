package winpipe

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/Microsoft/go-winio"
)

type PipeServer struct {
	name     string
	Listener net.Listener
	events   chan Config
}

type Event struct {
	Data string
}

type Config struct {
	ConfigData string `json:"configData"`
}

func NewWindowsPipe(pipeName string) (*PipeServer, error) {
	newPipe := new(PipeServer)
	var err error

	newPipe.name = pipeName
	newPipe.Listener, err = createPipe(pipeName)

	if err != nil {
		return nil, err
	}

	newPipe.events = make(chan Config)

	return newPipe, nil
}

func (p *PipeServer) Events() <-chan Config {
	return p.events
}

// type Service struct{
// 	events chan Config
// }

func (s *PipeServer) RunPipeListener(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /config", s.handleConfig)

	srv := &http.Server{
		Handler: mux,

		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	err := srv.Serve(s.Listener)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}

	close(s.events)
	return err
}

func (s *PipeServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	var cfg Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, "Invalid config: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Println("Received ", cfg.ConfigData)

	select {
	case s.events <- cfg: // Success
		w.WriteHeader(http.StatusNoContent)
	case <-r.Context().Done(): // Client disconnected
		return
	}
}

// SYSTEM and users in admin group: full control.
// Users: read and write. Network logons: denied.
const pipeSDDL = "D:P(D;;GA;;;NU)(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGW;;;BU)"

func createPipe(pipeName string) (net.Listener, error) {
	// TODO: Validate pipe name here and add security descriptor
	l, err := winio.ListenPipe(pipeName, &winio.PipeConfig{
		SecurityDescriptor: pipeSDDL}) // Creates the actual pipe

	if err != nil {
		return nil, err
	}

	return l, nil
}
