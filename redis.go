package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// it'd be nice to get rid of this and just use redis.Message generic interface
// ...but sigh Go: https://github.com/garyburd/redigo/issues/51
//
// let's just keep this and warp, even though there is some slight copy overhead
// probably, but it's better than having to have to have the rest of the code
// differentiate between structs based on where they originated.

// RedisMsg is our wrapper around redis.Message and redis.PMessage, since they
// really should be the same.
type RedisMsg struct {
	channel string
	data    []byte
}

// direct copy from http://godoc.org/github.com/garyburd/redigo/redis#Pool
// why do we need to cut and paste code instead of having it be built-in
// to the package?  because golang!
func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// this should make a global var we can access wherever?
var (
	redisPool *redis.Pool
)

func initRedisPool() {
	host, pass := envRedis()
	redisPool = newPool(host, pass)
}

func myRedisSubscriptions() (<-chan RedisMsg, <-chan RedisMsg, <-chan RedisMsg, <-chan RedisMsg, <-chan RedisMsg) {

	// set up structures and channels to stream events out on
	prolificUpdates := make(chan RedisMsg)
	mentionedUpdates := make(chan RedisMsg)
	hashtagUpdates := make(chan RedisMsg)
	recentTweetsUpdates := make(chan RedisMsg)
	tweetCountUpdates := make(chan RedisMsg)

	// get a new redis connection from pool.
	// since this is the first time the app tries to do something with redis,
	// die if we can't get a valid connection, since something is probably
	// configured wrong.
	conn := redisPool.Get()
	_, err := conn.Do("PING")
	if err != nil {
		log.Fatal("Could not connect to Redis, check your configuration.")
	}

	// subscribe to and handle streams
	psc := redis.PubSubConn{conn}
	psc.Subscribe("stream.prolific_updates")
	conn2 := redisPool.Get()
	psc2 := redis.PubSubConn{conn2}
	psc2.Subscribe("stream.mentioned_updates")
	conn3 := redisPool.Get()
	psc3 := redis.PubSubConn{conn3}
	psc3.Subscribe("stream.hashtag_updates")
	conn4 := redisPool.Get()
	psc4 := redis.PubSubConn{conn4}
	psc4.Subscribe("stream.recent_tweets_updates")
	conn5 := redisPool.Get()
	psc5 := redis.PubSubConn{conn5}
	psc5.Subscribe("stream.tweet_count_updates")

	go func() {
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				//fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
				prolificUpdates <- RedisMsg{v.Channel, v.Data}
			case error:
				log.Println("redis subscribe connection errored?@&*(#)akjd")
				// probable cause is connection was closed, but force close just in case
				conn.Close()

				log.Println("attempting to get a new one in 5 seconds...")
				time.Sleep(5 * time.Second)
				conn = redisPool.Get()
			}
		}
	}()
	
	go func() {
		for {
			switch v := psc2.Receive().(type) {
			case redis.Message:
				//fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
				mentionedUpdates <- RedisMsg{v.Channel, v.Data}
			case error:
				log.Println("redis subscribe connection errored?@&*(#)akjd")
				// probable cause is connection was closed, but force close just in case
				conn2.Close()

				log.Println("attempting to get a new one in 5 seconds...")
				time.Sleep(5 * time.Second)
				conn2 = redisPool.Get()
			}
		}
	}()
	
	go func() {
		for {
			switch v := psc3.Receive().(type) {
			case redis.Message:
				//fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
				hashtagUpdates <- RedisMsg{v.Channel, v.Data}
			case error:
				log.Println("redis subscribe connection errored?@&*(#)akjd")
				// probable cause is connection was closed, but force close just in case
				conn3.Close()

				log.Println("attempting to get a new one in 5 seconds...")
				time.Sleep(5 * time.Second)
				conn3 = redisPool.Get()
			}
		}
	}()
	
	go func() {
		for {
			switch v := psc4.Receive().(type) {
			case redis.Message:
				//fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
				recentTweetsUpdates <- RedisMsg{v.Channel, v.Data}
			case error:
				log.Println("redis subscribe connection errored?@&*(#)akjd")
				// probable cause is connection was closed, but force close just in case
				conn4.Close()

				log.Println("attempting to get a new one in 5 seconds...")
				time.Sleep(5 * time.Second)
				conn4 = redisPool.Get()
			}
		}
	}()
	
	go func() {
		for {
			switch v := psc5.Receive().(type) {
			case redis.Message:
				//fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
				tweetCountUpdates <- RedisMsg{v.Channel, v.Data}
			case error:
				log.Println("redis subscribe connection errored?@&*(#)akjd")
				// probable cause is connection was closed, but force close just in case
				conn5.Close()

				log.Println("attempting to get a new one in 5 seconds...")
				time.Sleep(5 * time.Second)
				conn5 = redisPool.Get()
			}
		}
	}()

	return prolificUpdates, mentionedUpdates, hashtagUpdates, recentTweetsUpdates, tweetCountUpdates
}
