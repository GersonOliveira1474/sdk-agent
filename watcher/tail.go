package watcher

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type LineHandler func(line string)

type Tail struct {
	filePath string
	handler  LineHandler
	stopCh   chan struct{}
	mu       sync.Mutex
	running  bool
	position int64
}

func New(filePath string, handler LineHandler) *Tail {
	return &Tail{
		filePath: filePath,
		handler:  handler,
		stopCh:   make(chan struct{}),
	}
}

func (t *Tail) Position() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.position
}

func (t *Tail) FilePath() string {
	return t.filePath
}

func (t *Tail) SetFile(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.filePath = path
	t.position = 0
}

func (t *Tail) ReadBacklog(n int) ([]string, error) {
	f, err := os.Open(t.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if n > 0 && len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	info, err := f.Stat()
	if err == nil {
		t.mu.Lock()
		t.position = info.Size()
		t.mu.Unlock()
	}

	return lines, scanner.Err()
}

func (t *Tail) Start() {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.running = true
	t.stopCh = make(chan struct{})
	t.mu.Unlock()

	go t.poll()
}

func (t *Tail) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.running {
		return
	}
	t.running = false
	close(t.stopCh)
}

func (t *Tail) IsRunning() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.running
}

func (t *Tail) poll() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.readNewLines()
		}
	}
}

func (t *Tail) readNewLines() {
	t.mu.Lock()
	pos := t.position
	path := t.filePath
	t.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return
	}

	if info.Size() < pos {
		t.mu.Lock()
		t.position = 0
		pos = 0
		t.mu.Unlock()
	}

	if info.Size() == pos {
		return
	}

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return
	}

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			t.handler(line)
		}
	}

	newPos, _ := f.Seek(0, io.SeekCurrent)
	t.mu.Lock()
	t.position = newPos
	t.mu.Unlock()
}
