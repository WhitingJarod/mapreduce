package main

import (
	"log"
	"net/http"
)

func main() {
    splitDatabase("austen.db", []string{"austen-0.db", "austen-1.db", "austen-2.db", "austen-3.db", "austen-4.db"})
    tempdir := ""
    address := "127.0.0.1:5000"
    go func() {
    http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
    if err := http.ListenAndServe(address, nil); err != nil {
        log.Printf("Error in HTTP server for %s: %v", address, err)
    }
}()
    _, err := mergeDatabases([]string{"http://127.0.0.1:5000/data/austen-0.db","http://127.0.0.1:5000/data/austen-1.db","http://127.0.0.1:5000/data/austen-2.db","http://127.0.0.1:5000/data/austen-3.db","http://127.0.0.1:5000/data/austen-4.db"}, "austen2.db", "tempy")
    if err != nil {
        log.Printf("Error merging databases: %v", err)
    }
}
