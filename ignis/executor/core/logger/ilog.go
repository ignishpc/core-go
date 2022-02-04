package logger

import (
	"io"
	"log"
	"os"
)

var debug = log.New(io.Discard, "[DEBUG] ", log.LstdFlags|log.Lshortfile)
var info = log.New(io.Discard, "[INFO]  ", log.LstdFlags|log.Lshortfile)
var warn = log.New(io.Discard, "[WARN]  ", log.LstdFlags|log.Lshortfile)
var error = log.New(io.Discard, "[ERROR] ", log.LstdFlags|log.Lshortfile)

func Init() {
	Enable(true)
}

func Enable(flag bool) {
	if flag {
		debug.SetOutput(os.Stderr)
		info.SetOutput(os.Stderr)
		warn.SetOutput(os.Stderr)
		error.SetOutput(os.Stderr)
	} else {
		debug.SetOutput(io.Discard)
		info.SetOutput(io.Discard)
		warn.SetOutput(io.Discard)
		error.SetOutput(io.Discard)
	}
}

func Debug(args ...any) {
	debug.Println(args...)
}

func Info(args ...any) {
	info.Println(args...)
}

func Warn(args ...any) {
	warn.Println(args...)
}

func Error(args ...any) {
	error.Println(args...)
}
