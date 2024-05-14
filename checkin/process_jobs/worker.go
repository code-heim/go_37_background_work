package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
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

type User struct {
	ID    int64
	Email string
	Name  string
}

type Context struct {
	currentUser *User
}

func (c *Context) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	fmt.Println("Starting a new job: ", job.Name, " with ID: ", job.ID)
	return next()
}

// Middleware to fetch the User object from userID
func (c *Context) FindCurrentUser(job *work.Job, next work.NextMiddlewareFunc) error {
	// If there's a user_id param
	if _, ok := job.Args["userID"]; ok {
		userID := job.ArgInt64("userID")
		// FIXME: Query the DB and get the user
		c.currentUser = &User{ID: userID, Email: "test" + strconv.Itoa(int(userID)) + "@codeheim.io", Name: "Test User"}
		if err := job.ArgError(); err != nil {
			return err
		}
	}

	return next()
}

// Create job enqueuer
var enqueuer = work.NewEnqueuer("demo_app", redisPool)

func main() {
	pool := work.NewWorkerPool(Context{}, 10, "demo_app", redisPool)

	// Middlewares
	pool.Middleware((*Context).Log)
	pool.Middleware((*Context).FindCurrentUser)

	// Name to job map
	pool.JobWithOptions("email",
		work.JobOptions{Priority: 10, MaxFails: 1}, (*Context).SendEmail)
	pool.JobWithOptions("report",
		work.JobOptions{Priority: 10, MaxFails: 1}, (*Context).Report)

	pool.Start()

	// Wait for a signal to quit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

func (c *Context) SendEmail(job *work.Job) error {
	addr := c.currentUser.Email
	subject := job.ArgString("subject")
	if err := job.ArgError(); err != nil {
		return err
	}

	fmt.Println("Sending mail to " + addr + " with subject " + subject)
	time.Sleep(time.Second * 2)
	return nil
}

func (c *Context) Report(job *work.Job) error {
	fmt.Println("Preparing report...")
	for i := range 360 {
		time.Sleep(time.Second * 10)
		job.Checkin("i = " + fmt.Sprint(i))
	}
	// Send the report via Email
	enqueuer.Enqueue("email",
		work.Q{"userID": c.currentUser.ID, "subject": "Report is ready!"})
	return nil
}
