package sink

import (
	"io"
)

type LogLevel int8

const (
	QUIET = LogLevel(iota)
	ERROR
	WARN
	INFO
	DEBUG
	TRACE
)

var (
	level  LogLevel    = INFO
	format string      = ""
	sinks  []io.Writer = nil
)

// Increments log level by 1 up to max of TRACE
func IncLogLevel() {
	level = min(TRACE, level+1)
}

// Decrements log level by 1 to a min of QUIET
func DecLogLevel() {
	level = max(QUIET, level-1)
}

// Sets log level directly
func SetLogLevel(l LogLevel) {
	level = l
}

// Appends writers to the set of sinks
func PushSinks(w ...io.Writer) {
	sinks = append(sinks, w...)
}

// Pops the first sinks and returns it
func PopSinks() io.Writer {
	s := sinks[0]
	sinks = sinks[1:]
	return s
}

// Sets sinks to nil
func FlushSinks() {
	sinks = nil
}

func SetFormat() error {
	return nil
}

// Writes the set of bytes to the sink, exits early in event of error and returns it
func Write(b []byte) error {
	for _, w := range sinks {
		_, err := w.Write(b)
		if err != nil {
			return err
		}
	}

	return nil
}
