package model

import (
	"strconv"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/bytedance/gopkg/util/gopool"
)

var (
	// shutdown signal channel
	logBufferShutdown chan struct{}
)

const (
	// Default buffer size for log channel
	DefaultLogBufferSize = 10000
	// Default flush interval in seconds
	DefaultLogFlushInterval = 5
	// Default batch size for DB inserts
	DefaultLogBatchSize = 100
)

var (
	// Buffered channel for async log writes
	logBuffer     chan *Log
	logBufferOnce sync.Once

	// Configuration
	logBufferSize    int
	logFlushInterval int
	logBatchSize     int
	asyncLogEnabled  bool
)

// InitLogBuffer initializes the async log buffer system
// Should be called from main.go after DB is initialized
func InitLogBuffer() {
	logBufferOnce.Do(func() {
		// Get config from env or use defaults
		logBufferSize = common.GetEnvOrDefault("LOG_BUFFER_SIZE", DefaultLogBufferSize)
		logFlushInterval = common.GetEnvOrDefault("LOG_FLUSH_INTERVAL", DefaultLogFlushInterval)
		logBatchSize = common.GetEnvOrDefault("LOG_BATCH_SIZE", DefaultLogBatchSize)

		logBuffer = make(chan *Log, logBufferSize)
		logBufferShutdown = make(chan struct{})
		asyncLogEnabled = true

		common.SysLog("async log buffer enabled with buffer size " +
			strconv.Itoa(logBufferSize) + ", flush interval " +
			strconv.Itoa(logFlushInterval) + "s, batch size " +
			strconv.Itoa(logBatchSize))

		// Start the background flush worker
		gopool.Go(func() {
			logFlushWorker()
		})
	})
}

// IsAsyncLogEnabled returns whether async logging is enabled
func IsAsyncLogEnabled() bool {
	return asyncLogEnabled && logBuffer != nil
}

// AddLogAsync adds a log entry to the buffer for async processing
// Returns true if successfully added, false if buffer is full (will fallback to sync)
func AddLogAsync(log *Log) bool {
	if !IsAsyncLogEnabled() {
		return false
	}

	select {
	case logBuffer <- log:
		return true
	default:
		// Buffer is full, caller should fallback to sync write
		return false
	}
}

// logFlushWorker is the background worker that flushes logs to DB
func logFlushWorker() {
	ticker := time.NewTicker(time.Duration(logFlushInterval) * time.Second)
	defer ticker.Stop()

	batch := make([]*Log, 0, logBatchSize)

	for {
		select {
		case <-logBufferShutdown:
			// Graceful shutdown - flush remaining logs
			common.SysLog("log buffer shutting down, flushing remaining logs...")
			if len(batch) > 0 {
				flushLogBatch(batch)
			}
			// Drain channel completely
			for {
				select {
				case log := <-logBuffer:
					batch = append(batch, log)
					if len(batch) >= logBatchSize {
						flushLogBatch(batch)
						batch = make([]*Log, 0, logBatchSize)
					}
				default:
					if len(batch) > 0 {
						flushLogBatch(batch)
					}
					common.SysLog("log buffer shutdown complete")
					return
				}
			}

		case log := <-logBuffer:
			batch = append(batch, log)

			// Flush if batch is full
			if len(batch) >= logBatchSize {
				flushLogBatch(batch)
				batch = make([]*Log, 0, logBatchSize)
			}

		case <-ticker.C:
			// Periodic flush even if batch isn't full
			if len(batch) > 0 {
				flushLogBatch(batch)
				batch = make([]*Log, 0, logBatchSize)
			}

			// Also drain any remaining logs in the channel
			drainCount := 0
			for {
				select {
				case log := <-logBuffer:
					batch = append(batch, log)
					drainCount++
					if len(batch) >= logBatchSize {
						flushLogBatch(batch)
						batch = make([]*Log, 0, logBatchSize)
					}
				default:
					goto drained
				}
			}
		drained:
			if len(batch) > 0 {
				flushLogBatch(batch)
				batch = make([]*Log, 0, logBatchSize)
			}
			if drainCount > 0 && common.DebugEnabled {
				common.SysLog("log buffer drained " + strconv.Itoa(drainCount) + " entries")
			}
		}
	}
}

// ShutdownLogBuffer gracefully shuts down the log buffer, flushing remaining logs
// Should be called before application exit
func ShutdownLogBuffer() {
	if !IsAsyncLogEnabled() {
		return
	}
	close(logBufferShutdown)
	// Give the worker a moment to flush
	time.Sleep(time.Duration(logFlushInterval+1) * time.Second)
}

// flushLogBatch writes a batch of logs to the database
func flushLogBatch(batch []*Log) {
	if len(batch) == 0 {
		return
	}

	// Use batch insert for efficiency
	err := LOG_DB.CreateInBatches(batch, len(batch)).Error
	if err != nil {
		common.SysError("failed to flush log batch: " + err.Error())
		// On error, try to insert one by one to salvage what we can
		for _, log := range batch {
			if insertErr := LOG_DB.Create(log).Error; insertErr != nil {
				common.SysError("failed to insert log: " + insertErr.Error())
			}
		}
	}

	if common.DebugEnabled {
		common.SysLog("flushed " + strconv.Itoa(len(batch)) + " logs to database")
	}
}

// GetLogBufferStats returns current buffer statistics
func GetLogBufferStats() map[string]interface{} {
	if !IsAsyncLogEnabled() {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	return map[string]interface{}{
		"enabled":        true,
		"buffer_size":    logBufferSize,
		"buffer_used":    len(logBuffer),
		"flush_interval": logFlushInterval,
		"batch_size":     logBatchSize,
	}
}
