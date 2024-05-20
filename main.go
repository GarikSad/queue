package main

import (
	"container/list"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"

	"time"
)

type Queue struct {
	queues map[string]*list.List
	mu     sync.Mutex
	cond   map[string]*sync.Cond
}

func NewQueueManager() *Queue {
	return &Queue{
		queues: make(map[string]*list.List),
		cond:   make(map[string]*sync.Cond),
	}
}
func (qm *Queue) Put(queueName, value string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[queueName]; !exists {
		qm.queues[queueName] = list.New()
		qm.cond[queueName] = sync.NewCond(&qm.mu)
	}

	qm.queues[queueName].PushBack(value)
	qm.cond[queueName].Broadcast()
	fmt.Println("In the List ", qm.queues)
}

func (qm *Queue) Get(queueName string, timeout int) (string, bool) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[queueName]; !exists {
		qm.queues[queueName] = list.New()
		qm.cond[queueName] = sync.NewCond(&qm.mu)
	}

	if elem := qm.queues[queueName].Front(); elem != nil {
		qm.queues[queueName].Remove(elem)
		return elem.Value.(string), true
	}

	if timeout > 0 {
		done := make(chan struct{})
		go func() {
			qm.cond[queueName].Wait()
			close(done)
		}()

		select {
		case <-done:
			if elem := qm.queues[queueName].Front(); elem != nil {
				qm.queues[queueName].Remove(elem)
				return elem.Value.(string), true
			}
		case <-time.After(time.Duration(timeout) * time.Second):
			return "", false
		}
	}

	return "", false
}
func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	queueManager := NewQueueManager()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.URL.Path[1:]
		switch r.Method {
		case http.MethodPut:
			value := r.URL.Query().Get("v")
			slog.Info(fmt.Sprintf("Getting Message %s from queue %s", value, queueName))
			if value == "" {
				http.Error(w, "Bad Request: Missing parameter 'v'", http.StatusBadRequest)
				return
			}
			queueManager.Put(queueName, value)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			timeoutStr := r.URL.Query().Get("timeout")
			timeout := 0
			if timeoutStr != "" {
				var err error
				timeout, err = strconv.Atoi(timeoutStr)
				if err != nil {
					http.Error(w, "Bad Request: Invalid 'timeout' parameter", http.StatusBadRequest)
					return
				}
			}
			if value, ok := queueManager.Get(queueName, timeout); ok {
				w.Write([]byte(value))
			} else {
				http.Error(w, "Not Found", http.StatusNotFound)
			}
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Starting server on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
