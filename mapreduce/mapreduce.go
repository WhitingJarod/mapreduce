package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unicode"
)

func LOG(format string, args ...interface{}) {
    log.Printf(format, args...)
}

func ERR(format string, args ...interface{}) {
    log.Printf("\033[91m" + format + "\033[0m", args...)
}

type MapTask struct {
    NumMapTasks, NumReduceTasks int    // total number of map and reduce tasks
    TaskId                      int    // map task number, 0-based
    SourceHost                  string // address of host with map input file
}

type ReduceTask struct {
    NumMapTasks, NumReduceTasks int      // total number of map and reduce tasks
    TaskId                      int      // reduce task number, 0-based
    SourceHosts                 []string // addresses of map workers
}

type Pair struct {
    Key   string
    Value string
}

type Interface interface {
    Map(key, value string, output chan<- Pair) error
    Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func getLocalAddress() string {
    conn, err := net.Dial("udp", "8.8.8.8:80")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    localAddr := conn.LocalAddr().(*net.UDPAddr)

    localaddress := localAddr.IP.String()

    if localaddress == "" {
        panic("init: failed to find non-loopback interface with valid address on this node")
    }
    return localaddress
}

func use(_... interface{}){}

func Start() {

//     use(use)
//     runtime.GOMAXPROCS(1)
//     m := 10
//     r := 5
//     source := "source.db"
//     //target := "target.db"
//     tmp := os.TempDir()

//     tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))
//     if err := os.RemoveAll(tempdir); err != nil {
//         log.Fatalf("unable to delete old temp dir: %v", err)
//     }
//     if err := os.Mkdir(tempdir, 0700); err != nil {
//         log.Fatalf("unable to create temp dir: %v", err)
//     }
//     defer os.RemoveAll(tempdir)

//     log.Printf("splitting %s into %d pieces", source, m)
//     var paths []string
//     for i := 0; i < m; i++ {
//         paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
//     }
//     if err := splitDatabase(source, paths); err != nil {
//         log.Fatalf("splitting database: %v", err)
//     }

//     myAddress := net.JoinHostPort(getLocalAddress(), "3410")
//     log.Printf("starting http server at %s", myAddress)
//     listener, err := net.Listen("tcp", myAddress)
//     http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
//     if err != nil {
//         log.Fatalf("Listen error on address %s: %v", myAddress, err)
//     }
//     go func() {
//         if err := http.Serve(listener, nil); err != nil {
//             log.Fatalf("Serve error: %v", err)
//         }
//     }()
//     // go func() {
//     //     http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
//     //     if err := http.ListenAndServe(myAddress, nil); err != nil {
//     //         log.Fatalf("Error in HTTP server for %s: %v", myAddress, err)
//     //     }
//     // }()

//     // build the map tasks
//     var mapTasks []*MapTask
//     for i := 0; i < m; i++ {
//         task := &MapTask{
//             NumMapTasks:    m,
//             NumReduceTasks: r,
//             TaskId:         i,
//             SourceHost:     myAddress,
//         }
//         mapTasks = append(mapTasks, task)
//     }

//     // build the reduce tasks
//     var reduceTasks []*ReduceTask
//     for i := 0; i < r; i++ {
//         task := &ReduceTask{
//             NumMapTasks:    m,
//             NumReduceTasks: r,
//             TaskId:         i,
//             SourceHosts:    make([]string, m),
//         }
//         reduceTasks = append(reduceTasks, task)
//     }
//     var client Client

//     // process the map tasks
//     for i, task := range mapTasks {
//         if err := task.Process(tempdir, client); err != nil {
//             log.Fatalf("processing map task %d: %v", i, err)
//         }
//         for _, reduce := range reduceTasks {
//             reduce.SourceHosts[i] = makeURL(myAddress, mapOutputFile(i, reduce.TaskId))
//         }
//     }

//     // process the reduce tasks
//     for i, task := range reduceTasks {
//         if err := task.Process(tempdir, client); err != nil {
//             log.Fatalf("processing reduce task %d: %v", i, err)
//         }
//     }

//     // gather outputs into final target.db file
//     target, err := createDatabase("target.db")
//     if err != nil {
//         log.Fatalf("creating target database: %v", err)
//     }
//     defer target.Close()
//     for i := 0; i < r; i++ {
//         err := gatherInto(target, filepath.Join(tempdir, reduceOutputFile(i)))
//         if err != nil {
//             log.Fatalf("gathering reduce output %d: %v", i, err)
//         }
//     }
}
