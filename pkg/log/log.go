/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	Storage    = "stor_name"
	Object     = "obj_name"
	Bucket     = "bucket"
	Method     = "method"
	user       = "user"
	TraceID    = "trace_id"
	httpPath   = "http_path"
	grpcMethod = "grpc_method"
	httpMethod = "http_method"
	httpQuery  = "http_query"
	flow       = "flow"
)

type Config struct {
	Json  bool        `yaml:"json"`
	Level string      `yaml:"level"`
	File  *FileConfig `yaml:"file,omitempty"`
}

// FileConfig configures optional, additional file logging. File logging is
// disabled unless Path is set.
type FileConfig struct {
	Path  string `yaml:"path"`
	Level string `yaml:"level"`
	Json  bool   `yaml:"json"`
}

func parseLevel(level string) zerolog.Level {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return zerolog.InfoLevel
	}
	return lvl
}

func newWriter(w io.Writer, json bool) io.Writer {
	if json {
		return w
	}
	return zerolog.ConsoleWriter{
		Out:        w,
		TimeFormat: time.RFC3339,
	}
}

// leveledWriter drops events below level, so writers with different levels
// can be combined with zerolog.MultiLevelWriter.
type leveledWriter struct {
	io.Writer
	level zerolog.Level
}

func (w leveledWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	if level < w.level {
		return len(p), nil
	}
	return w.Write(p)
}

// fileWriters caches opened log files by path. CreateLogger is called per
// request/task by the grpc/http/worker middlewares, so the file must be
// opened once and reused rather than reopened on every call.
var (
	fileWritersMu sync.Mutex
	fileWriters   = map[string]*os.File{}
)

func getFileWriter(path string) (*os.File, error) {
	fileWritersMu.Lock()
	defer fileWritersMu.Unlock()
	if f, ok := fileWriters[path]; ok {
		return f, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	fileWriters[path] = f
	return f, nil
}

func GetLogger(cfg *Config, app, appID string) (zerolog.Logger, error) {
	logger, err := CreateLogger(cfg, app, appID)
	if err != nil {
		return zerolog.Logger{}, err
	}
	zerolog.DefaultContextLogger = &logger
	return logger, nil
}

func CreateLogger(cfg *Config, app, appID string) (zerolog.Logger, error) {
	stdoutLevel := parseLevel(cfg.Level)
	writers := []io.Writer{leveledWriter{newWriter(os.Stdout, cfg.Json), stdoutLevel}}
	minLevel := stdoutLevel

	if cfg.File != nil && cfg.File.Path != "" {
		f, err := getFileWriter(cfg.File.Path)
		if err != nil {
			return zerolog.Logger{}, fmt.Errorf("%w: unable to open log file %s", err, cfg.File.Path)
		}
		fileLevelStr := cfg.File.Level
		if fileLevelStr == "" {
			fileLevelStr = cfg.Level
		}
		fileLevel := parseLevel(fileLevelStr)
		writers = append(writers, leveledWriter{newWriter(f, cfg.File.Json), fileLevel})
		if fileLevel < minLevel {
			minLevel = fileLevel
		}
	}

	zerolog.SetGlobalLevel(minLevel)
	logger := zerolog.New(zerolog.MultiLevelWriter(writers...))
	l := logger.With().Caller().Timestamp()
	if appID != "" {
		l = l.Str("app_id", appID)
	}
	if app != "" {
		l = l.Str("app", app)
	}

	logger = l.Logger()
	return logger, nil
}
