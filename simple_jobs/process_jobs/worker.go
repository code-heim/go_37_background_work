package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

// Redis pool
var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle:   5,
	Wait:      true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379")
	},
}

type Context struct {
}

func main() {
	pool := work.NewWorkerPool(Context{}, 10, "demo_app", redisPool)

	// Name to job map
	pool.JobWithOptions("email",
		work.JobOptions{Priority: 10, MaxFails: 1}, SendEmail)

	pool.Start()

	// Wait for a signal to quit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

func SendEmail(job *work.Job) error {
	addr := job.ArgString("email")
	subject := job.ArgString("subject")
	if err := job.ArgError(); err != nil {
		return err
	}

	fmt.Println("Sending mail to " + addr + " with subject " + subject)
	time.Sleep(time.Second * 2)
	return nil
}
