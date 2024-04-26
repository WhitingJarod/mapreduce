package main

import (
    "log"
    "mapreduce"
    "strconv"
    "strings"
    "unicode"
)

func main() {
    var c Client
    if err := mapreduce.Start(c); err != nil {
        log.Fatalf("%v", err)
    }
}

// Map and Reduce functions for a basic wordcount client

type Client struct{}

func (c Client) Map(key, value string, output chan<- mapreduce.Pair) error {
    defer close(output)
    lst := strings.Fields(value)
    for _, elt := range lst {
        word := strings.Map(func(r rune) rune {
            if unicode.IsLetter(r) || unicode.IsDigit(r) {
                return unicode.ToLower(r)
            }
            return -1
        }, elt)
        if len(word) > 0 {
            output <- mapreduce.Pair{Key: word, Value: "1"}
        }
    }
    return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- mapreduce.Pair) error {
    defer close(output)
    count := 0
    for v := range values {
        i, err := strconv.Atoi(v)
        if err != nil {
            return err
        }
        count += i
    }
    p := mapreduce.Pair{Key: key, Value: strconv.Itoa(count)}
    output <- p
    return nil
}
