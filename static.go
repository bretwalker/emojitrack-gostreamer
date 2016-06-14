// This is sofucking hacky to set up another http server, but
// it beats setting up a django app to handle initial page views.

package main

import (
    "encoding/json"
    "fmt"
//    "io"
//    "io/ioutil"
    "log"
//    "math"
    "net/http"
    "os"
//    "sort"
    "strconv"
    "time"
    "strings"
    "github.com/garyburd/redigo/redis"
)

type Tweet struct {
    PossiblySensitive bool  `json:"possibly_sensitive"`
    Text string             `json:"text"`
    ScreenName string       `json:"screen_name"`
    ProfileImageUrl string  `json:"profile_image_url"`
    CreatedAt time.Time     `json:"created_at"`
    ID uint64               `json:"id"`
}

type ScoredThing struct {
    Name string             `json:"name"`
    Score int64            `json:"score"`
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
    conn := redisPool.Get()
	_, err := conn.Do("PING")
	if err != nil {
		log.Fatal("Could not connect to Redis, check your configuration.")
	}
	
	n, err := conn.Do("LRANGE", "recent_tweets", "0", "-1")
    s, _ := redis.Strings(n, err)
	var tweets = make([]Tweet,len(s))
    for i := 0; i < len(s); i++ {
        var tweet Tweet
        json.Unmarshal([]byte(s[i]), &tweet)
        tweets[i] = tweet
    }
    j, _ := json.Marshal(tweets)
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    w.Header().Set("Content-Type", "application/json")
    fmt.Fprint(w, string(j))
}

func prolificHandler(w http.ResponseWriter, r *http.Request) {	
	days, daysOK := strconv.ParseInt(r.FormValue("days"), 0, 64)
    
    if daysOK != nil || days < 0 || days > 2 {
        http.Error(w, "403 Forbidden: Give us what we need!", 403)
        return
    }
    
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    w.Header().Set("Content-Type", "application/json")
    fmt.Fprint(w, getScoresFromRedis("prolific", days))
}

func mentionedHandler(w http.ResponseWriter, r *http.Request) {	
	days, daysOK := strconv.ParseInt(r.FormValue("days"), 0, 64)
    
    if daysOK != nil || days < 0 || days > 2 {
        http.Error(w, "403 Forbidden: Give us what we need!", 403)
        return
    }
    
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    w.Header().Set("Content-Type", "application/json")
    fmt.Fprint(w, getScoresFromRedis("mentioned", days))
}

func hashtagHandler(w http.ResponseWriter, r *http.Request) {	
	days, daysOK := strconv.ParseInt(r.FormValue("days"), 0, 64)
    
    if daysOK != nil || days < 0 || days > 2 {
        http.Error(w, "403 Forbidden: Give us what we need!", 403)
        return
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    fmt.Fprint(w, getScoresFromRedis("hashtag", days))
}

func profilePicHandler(w http.ResponseWriter, r *http.Request) {	
    r.ParseForm()
	var screenNames = strings.Split(r.FormValue("n"), ",")

    if (len(screenNames) == 1 && screenNames[0] == "") || len(screenNames) > 1000 {
        http.Error(w, "403 Forbidden: Give us what we need!", 403)
        return
    }
    
    conn := redisPool.Get()
    _, err := conn.Do("PING")
    if err != nil {
    	log.Fatal("Could not connect to Redis, check your configuration.")
    }

    var keys []interface{}
    for i := 0; i < len(screenNames); i++ {
       keys = append(keys, "pic_" + screenNames[i])
    }
    n, err := conn.Do("MGET", keys...)

    s, _ := redis.Strings(n, err)

    m := make(map[string]interface{})
    for i := 0; i < len(s); i++ {
        if(s[i] != "") {
            m[screenNames[i]] = s[i] // m["bret"] = "http://www.bretw.com/pretty.jpg"
        } else {
            m[screenNames[i]] = nil
        }
    }
    
    j, _ := json.Marshal(m)
    
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    fmt.Fprint(w, string(j))
}

func tweetCountHandler(w http.ResponseWriter, r *http.Request) {	
    conn := redisPool.Get()
    _, err := conn.Do("PING")
    if err != nil {
    	log.Fatal("Could not connect to Redis, check your configuration.")
    }

    n, err := conn.Do("GET", "tweet_count")

    tweet_count, _ := redis.Int64(n, err)
    
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "file://")
    fmt.Fprint(w, fmt.Sprintf("{\"tweet_count\": %d}", tweet_count))
}

func getScoresFromRedis(metricName string, days int64) (string) {

    key := metricName + "_score" // all time (default)
    switch days {
        case 1: // 7 days
            key = key + "_7drollup"
        case 2: // 30 days
            key = key + "_30drollup"
    }

    conn := redisPool.Get()
    _, err := conn.Do("PING")
    if err != nil {
    	log.Fatal("Could not connect to Redis, check your configuration.")
    }

    n, err := conn.Do("ZREVRANGEBYSCORE", key, "inf", "-inf", "WITHSCORES", "LIMIT", 0, 1000)

    s, _ := redis.Strings(n, err)
    a := make([]ScoredThing, len(s)/2)
    ai := 0
    for i := 0; i < len(s); i += 2 {
        score, _ := strconv.ParseInt(s[i+1], 0, 64)
        a[ai] = ScoredThing{Name: s[i], Score: score}
        ai++
    }
    j, _ := json.Marshal(a)

    return string(j)
}

func static_server() {
    s := &http.Server{
    	Addr:           ":8081",
    	Handler:        nil,
    	ReadTimeout:    10 * time.Second,
    	WriteTimeout:   10 * time.Second,
    	MaxHeaderBytes: 1 << 20,
    }
    
    http.HandleFunc("/recent", statusHandler)
    http.HandleFunc("/prolific", prolificHandler)
    http.HandleFunc("/mentioned", mentionedHandler)
    http.HandleFunc("/hashtag", hashtagHandler)
    http.HandleFunc("/profile-pic", profilePicHandler)
    http.HandleFunc("/tweet-count", tweetCountHandler)
    err :=  s.ListenAndServe()
    
    if err != nil {
        log.Println(err)
        os.Exit(1)
    }
}
