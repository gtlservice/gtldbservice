package systemutils_linux

import (
	"os"
	"syscall"
    "os/signal"
)

type SystemSigalSet struct {
	sigset map[os.Signal]interface{}
}

func WaitSignal(signal os.Signal) {
	c := make(chan os.Signal, 1)
	//拦截指定的系统信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
}
