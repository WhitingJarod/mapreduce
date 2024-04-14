package main

import (
	"database/sql"
	"fmt"
)


type MapTask struct {
    M, R int // total number of map and reduce tasks
    N int // map task number, 0-based
    SourceHost string // address of host with map input files
}

type ReduceTask struct {
    M, R int // total number of map and reduce tasks
    N int // reduce task number, 0-based
    SourceHosts []string // addresses of map workers
}

type Pair struct {
    Key string
    Value string
}

type Interface interface {
    Map(key, value string, output chan<-Pair) error
    Reduce(key string, values <-chan string, output chan<-Pair) error
}

func mapSourceFile(m int) string { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func (task *MapTask) Process(tempdir string, client Interface) error {
    // Download and open the input file
    sourceURL := makeURL(task.SourceHost, mapSourceFile(task.N))
    db, err := downloadDatabase(sourceURL, tempdir)
    if err != nil {
        return err
    }
    // Create the output files using createDatabase
    outputDBs := make([]*sql.DB, task.R)
    for r := 0; r < task.R; r++ {
        outputDB, err := createDatabase(mapOutputFile(task.N, r))
        if err != nil {
            return err
        }
        outputDBs[r] = outputDB
    }
}
