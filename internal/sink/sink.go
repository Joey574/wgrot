package sink

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
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

// Sets the logging format string to be used, by default this is empty
// \d => outputs the datetime
// \t => writes the log type
// * => log data
// example
// [\d] [\t] *
func SetFormat(f string) error {
	format = f
	return nil
}

// Writes the set of bytes to the sink, exits early in event of error and returns it
func Write(l LogLevel, b []byte) error {
	if l > level {
		return nil
	}

	data := formatString(string(b), l)
	return writeToSinks([]byte(data))
}

func WriteString(l LogLevel, s string) error {
	return Write(l, []byte(s))
}

func Printf(l LogLevel, format string, a ...any) error {
	return WriteString(l, fmt.Sprintf(format, a...))
}

func Println(l LogLevel, a ...any) error {
	return WriteString(l, fmt.Sprintln(a...))
}

func Print(l LogLevel, a ...any) error {
	return WriteString(l, fmt.Sprint(a...))
}

func Fatal(v ...any) {
	writeToSinks(fmt.Append(nil, v...))
	os.Exit(1)
}

func Fatalf(format string, a ...any) {
	writeToSinks(fmt.Appendf(nil, format, a...))
	os.Exit(1)
}

func Fatalln(v ...any) {
	writeToSinks(fmt.Appendln(nil, v...))
	os.Exit(1)
}

func writeToSinks(b []byte) error {
	for _, w := range sinks {
		_, err := w.Write(b)
		if err != nil {
			return err
		}
	}

	return nil
}

func formatString(data string, level LogLevel) string {
	if format == "" {
		return data
	}

	lvl := "UNKN"
	switch level {
	case QUIET:
		lvl = "QUIET"
	case ERROR:
		lvl = "ERROR"
	case WARN:
		lvl = "WARN"
	case INFO:
		lvl = "INFO"
	case DEBUG:
		lvl = "DEBUG"
	case TRACE:
		lvl = "TRACE"
	}

	out := strings.ReplaceAll(format, "*", data)
	out = strings.ReplaceAll(out, "\\d", time.Now().Format("2006-01-02 15:04:05"))
	return strings.ReplaceAll(out, "\\t", lvl)
}
