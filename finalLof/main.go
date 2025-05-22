package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	segmentPrefix  = "wal_"
	maxSegmentSize = 500
	walDir         = "wal_data"
	bufferSize     = 4096
)

type WAL struct {
	directory           string
	currentSegment      *os.File
	writer              *bufio.Writer
	currentSegmentIndex int
	offset              int64
	mu                  sync.Mutex
	encoder             *json.Encoder
}

type LogEntry struct {
	Offset  int         `json:"offset"`
	Topic   string      `json:"topic"`
	Payload interface{} `json:"payload"`
}

var segmentNameCache = make(map[string]string)
var segmentCacheMu sync.RWMutex

// ** genenrate a segment file name
func segmentFileName(directory string, index int) string {
	key := fmt.Sprintf("%s:%d", directory, index)
	segmentCacheMu.RLock()
	if name, exists := segmentNameCache[key]; exists {
		segmentCacheMu.RUnlock()
		return name
	}
	segmentCacheMu.RUnlock()
	name := filepath.Join(directory, fmt.Sprintf("%s%d.log", segmentPrefix, index))
	segmentCacheMu.Lock()
	segmentNameCache[key] = name
	segmentCacheMu.Unlock()
	return name
}

// ** find the last segment index
// ** if there is no segment file it will create a new one with index 1
// ** if there is a segment file it will return the last index
func findLastSegemtIndex(directory string) (int, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 1, nil
		// ** there is no directory so it will create a new one with index 1 for the first segment
	}
	maxIndex := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), segmentPrefix) {
			continue
		}

		name := entry.Name()
		indexStr := strings.TrimPrefix(name, segmentPrefix)
		indexStr = strings.TrimSuffix(indexStr, ".log")
		if index, err := strconv.Atoi(indexStr); err == nil && index > maxIndex {
			maxIndex = index
		}
	}
	if maxIndex == 0 {
		return 1, nil
	}
	return maxIndex, nil
}

// ** function to get stat of the file
// ** this will be used to get the size of the file
func calculateOffset(file *os.File) (int, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(stat.Size()), nil
}

func newWriteAheadLOG() (*WAL, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %v", err)
	}
	segementIndex, err := findLastSegemtIndex(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to find last segment index: %v", err)
	}

	segmentPath := segmentFileName(walDir, segementIndex)
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %v", err)
	}

	offset, err := calculateOffset(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to calculate offset: %v", err)
	}

	writer := bufio.NewWriterSize(file, bufferSize)
	wal := &WAL{
		directory:           walDir,
		currentSegment:      file,
		writer:              writer,
		currentSegmentIndex: segementIndex,
		offset:              1 + int64(offset),
	}

	wal.encoder = json.NewEncoder(writer)
	return wal, nil
}

func (w *WAL) FlushE() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %v", err)
	}
	if err := w.currentSegment.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment file: %v", err)
	}
	return nil
}

func (w *WAL) rotateSegment() error {
	if err := w.FlushE(); err != nil {
		return err
	}
	if err := w.currentSegment.Close(); err != nil {
		return err
	}

	// ** create a new segment file
	w.currentSegmentIndex++
	segmentPath := segmentFileName(w.directory, w.currentSegmentIndex)
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open new segment file: %v", err)
	}
	w.currentSegment = file
	w.writer = bufio.NewWriterSize(file, bufferSize)
	w.encoder = json.NewEncoder(w.writer)
	w.offset = w.offset + 1
	return nil
}

func (w *WAL) WriteLog(topic string, payload interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := LogEntry{
		Offset:  int(w.offset),
		Topic:   topic,
		Payload: payload,
	}
	if err := w.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to encode log entry: %v", err)
	}
	if err := w.FlushE(); err != nil {
		return fmt.Errorf("failed to flush log entry: %v", err)
	}

	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}
	currentFileSize := fileInfo.Size()
	w.offset = w.offset + 1
	if currentFileSize >= maxSegmentSize {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %v", err)
		}
	}
	return nil
}

func main() {
	wal, err := newWriteAheadLOG()
	if err != nil {
		fmt.Printf("Error creating WAL: %v\n", err)
		os.Exit(1)
		return
	}
	fmt.Println(wal)
	http.HandleFunc("/write", wal.ServerHTTP)
	fmt.Println("Server started on :9090")
	http.ListenAndServe(":9090", nil)

}

func (w *WAL) ServerHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		w.handleWrite(writer, request)
	default:
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
	}
}



func ( w *WAL) handleRead(writer http.ResponseWriter, request *http.Request){
	
}

// ** handle the write request
// ** this will be used to write the log entry to the file
func (w *WAL) handleWrite(writer http.ResponseWriter, request *http.Request) {
	var payload map[string]interface{}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		http.Error(writer, "Invalid payload", http.StatusBadRequest)
		return
	}
	topic := request.URL.Query().Get("topic")
	if topic == "" {
		topic = "default"
	}

	if err := w.WriteLog(topic, payload); err != nil {
		http.Error(writer, "Failed to write log", http.StatusInternalServerError)
		return
	}

	w.mu.Lock()
	currentOffset := w.offset
	currentSegment := w.currentSegmentIndex
	w.mu.Unlock()

	writer.WriteHeader(http.StatusCreated)
	json.NewEncoder(writer).Encode(map[string]interface{}{
		"offset":   currentOffset,
		"segment":  currentSegment,
		"topic":    topic,
		"payload":  payload,
		"message":  "Log entry written successfully",
		"fileSize": w.currentSegment.Name(),
	})
}
