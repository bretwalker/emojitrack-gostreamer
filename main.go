package main

import (
	"log"
	"time"
	"github.com/mroth/sseserver"
)

func main() {
    go func() {
        static_server()
    }()

	// set up SSE server interface
	s := sseserver.NewServer()
	clients := s.Broadcast

	// get us some data from redis
	log.Println("Connecting to Redis...")
	initRedisPool()
	prolificUpdates, mentionedUpdates, hashtagUpdates, recentTweetsUpdates, tweetCountUpdates := myRedisSubscriptions()

	prolificEpsFeeder := make(chan RedisMsg)
	mentionedEpsFeeder := make(chan RedisMsg)
	hashtagEpsFeeder := make(chan RedisMsg)
	tweetCountEpsFeeder := make(chan RedisMsg)

	go func() {
		for scoreUpdate := range prolificUpdates {
			prolificEpsFeeder <- scoreUpdate
		}
	}()

	go func() {
    	for scoreUpdate := range mentionedUpdates {
    		mentionedEpsFeeder <- scoreUpdate
    	}
	}()
	
	go func() {
    	for scoreUpdate := range hashtagUpdates {
    		hashtagEpsFeeder <- scoreUpdate
    	}
	}()
	
	go func() {
    	for scoreUpdate := range tweetCountUpdates {
    		tweetCountEpsFeeder <- scoreUpdate
    	}
	}()

	// Handle packing scores for eps namespace.
	//
	// This first goroutine basically grabs just the data field of a Redis msg,
	// and converts it to a string, because that's what my generic scorepacker
	// function expects to receive.
	//
	// Then, we just pipe that chan into a ScorePacker.
	prolificScoreVals := make(chan string)
	prolificEpsScoreUpdates := ScorePacker(prolificScoreVals, time.Duration(17*time.Millisecond))
	go func() {
		for {
			prolificScoreVals <- string((<-prolificEpsFeeder).data)
		}
	}()
	
	mentionedScoreVals := make(chan string)
	mentionedEpsScoreUpdates := ScorePacker(mentionedScoreVals, time.Duration(17*time.Millisecond))
	go func() {
		for {
			mentionedScoreVals <- string((<-mentionedEpsFeeder).data)
		}
	}()
	
	hashtagScoreVals := make(chan string)
	hashtagEpsScoreUpdates := ScorePacker(hashtagScoreVals, time.Duration(17*time.Millisecond))
	go func() {
		for {
			hashtagScoreVals <- string((<-hashtagEpsFeeder).data)
		}
	}()
	
	tweetCountScoreVals := make(chan string)
	tweetCountEpsScoreUpdates := ScorePacker(tweetCountScoreVals, time.Duration(17*time.Millisecond))
	go func() {
		for {
			tweetCountScoreVals <- string((<-tweetCountEpsFeeder).data)
		}
	}()
	
	// goroutines to handle passing messages to the proper connection pool.
	//
	// I could use a select here and do as one goroutine, but having each be
	// independent could be slightly better for concurrency as these actually do
	// have a small amount of overhead in creating the SSEMessage so this is
	// theoretically better if we are running in parallel on appropriate hardware.

	// epsPublisher
	go func() {
		for val := range prolificEpsScoreUpdates {
			clients <- sseserver.SSEMessage{
				Event:     "",
				Data:      val,
				Namespace: "/eps/prolific",
			}
		}
	}()
	
	go func() {
		for val := range mentionedEpsScoreUpdates {
			clients <- sseserver.SSEMessage{
				Event:     "",
				Data:      val,
				Namespace: "/eps/mentioned",
			}
		}
	}()
	
	go func() {
		for val := range hashtagEpsScoreUpdates {
			clients <- sseserver.SSEMessage{
				Event:     "",
				Data:      val,
				Namespace: "/eps/hashtag",
			}
		}
	}()
	
	go func() {
		for val := range tweetCountEpsScoreUpdates {
			clients <- sseserver.SSEMessage{
				Event:     "",
				Data:      val,
				Namespace: "/eps/tweet-count",
			}
		}
	}()
	
	go func() {
		for msg := range recentTweetsUpdates {
			clients <- sseserver.SSEMessage{
				Event:     "",
				Data:      msg.data,
				Namespace: "/recent-tweets",
			}
		}
	}()

	// start the monitor reporter to periodically send our status to redis
	go adminReporter(s)
	gorelicMonitor()

	// share and enjoy
	port := envPort()
	log.Println("Starting server on port " + port)
	log.Println("HOLD ON TO YOUR BUTTS...")

	// this method blocks by design
	s.Serve(port)
}
