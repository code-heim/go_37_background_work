package main

import (
	"log"

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

// Create job enqueuer
var enqueuer = work.NewEnqueuer("demo_app", redisPool)

func main() {
	_, err := enqueuer.Enqueue("email",
		work.Q{"userID": 10, "subject": "Just testing!"})
	if err != nil {
		log.Fatal(err)
	}

	enqueuer.Enqueue("report",
		work.Q{"userID": 5})
}
