package systemutils_linux

import (
	"os"
	"syscall"
    "os/signal"
)



func WaitSignal(signal os.Signal) {
	c := make(chan os.Signal, 1)
	//拦截指定的系统信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-c:
		if s != sig {
			t.Fatalf("signal was %v, want %v", s, sig)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for %v", sig)
	}
}
