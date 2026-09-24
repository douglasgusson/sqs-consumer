package storage

import (
	"encoding/json"
	"os"
	"sync"
)

// JSONLWriter serializa escritas concorrentes de várias workers.
type JSONLWriter struct {
	mu     sync.Mutex
	f      *os.File
	closed bool
}

func NewJSONLWriter(path string) (*JSONLWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &JSONLWriter{f: f}, nil
}

func (w *JSONLWriter) Write(r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return os.ErrClosed
	}

	_, err = w.f.Write(b) // uma única chamada = linha íntegra
	return err
}

func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
