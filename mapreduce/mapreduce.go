package mapreduce

import (
    "context"
    "flag"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "path/filepath"

    pb "mapreduce/proto"

    "google.golang.org/grpc"
)

func LOG(format string, args ...interface{}) {
    log.Printf(format, args...)
}

func ERR(format string, args ...interface{}) {
    log.Printf("\033[91m"+format+"\033[0m", args...)
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


type ProtocolServer struct {
    pb.UnimplementedMasterServer
}

func (s *ProtocolServer) RequestTask(ctx context.Context, req *pb.WorkerStatus) (*pb.Task, error) {
    return &pb.Task{}, nil
}

func use(_ ...interface{}) {}

func do_master(num_map_tasks, num_reduce_tasks int, input_file, output_file, my_address, my_port, temp_dir string) {
    LOG("master mode")
    LOG("num_map_tasks: %d", num_map_tasks)
    LOG("num_reduce_tasks: %d", num_reduce_tasks)
    LOG("input_file: %s", input_file)
    LOG("output_file: %s", output_file)
    LOG("my_address: %s", my_address)
    LOG("my_port: %s", my_port)

    // The master node needs to do the following:
    // 1. Split the input file and start an HTTP server to serve source chunks to map workers.
    split_paths := make([]string, num_map_tasks)
    for i := 0; i < num_map_tasks; i++ {
        split_paths[i] = filepath.Join(temp_dir, mapSourceFile(i))
    }
    if err := splitDatabase(input_file, split_paths); err != nil {
        ERR("splitDatabase: %v", err)
        return
    }
    myAddress := net.JoinHostPort(my_address, my_port)
    LOG("starting http server at %s", myAddress)
    listener, err := net.Listen("tcp", myAddress)
    if err != nil {
        ERR("Listen error on address %s: %v", myAddress, err)
        return
    }
    http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(temp_dir))))
    go func() {
        if err := http.Serve(listener, nil); err != nil {
            ERR("Serve error: %v", err)
        }
    }()

    // 2. Generate the full set of map tasks and reduce tasks. Note that reduce
    // tasks will be incomplete initially, because they require a list of the
    // hosts that handled each map task.
    map_tasks := make([]*MapTask, num_map_tasks)
    for i := 0; i < num_map_tasks; i++ {
        map_tasks[i] = &MapTask{
            NumMapTasks:    num_map_tasks,
            NumReduceTasks: num_reduce_tasks,
            TaskId:         i,
            SourceHost:     myAddress,
        }
    }

    reduce_tasks := make([]*ReduceTask, num_reduce_tasks)
    for i := 0; i < num_reduce_tasks; i++ {
        reduce_tasks[i] = &ReduceTask{
            NumMapTasks:    num_map_tasks,
            NumReduceTasks: num_reduce_tasks,
            TaskId:         i,
            SourceHosts:    make([]string, num_map_tasks),
        }
    }

    // 3. Create and start an RPC server to handle incoming client requests.
    // Note that it can use the same HTTP server that shares static files.
    server := &ProtocolServer{}
    grpcServer := grpc.NewServer()
    pb.RegisterMasterServer(grpcServer, server)
}

func do_client(master_address, master_port, my_address, my_port, temp_dir string) {
    LOG("client mode")
    LOG("master_address: %s", master_address)
    LOG("master_port: %s", master_port)
    LOG("my_address: %s", my_address)
    LOG("my_port: %s", my_port)
}

func Start() {
    use(use)

    is_master := flag.Bool("master", false, "master mode")
    num_map_tasks := flag.Int("m", 10, "number of map tasks (inherited from master)")
    num_reduce_tasks := flag.Int("r", 5, "number of reduce tasks (inherited from master)")
    input_file := flag.String("source", "source.db", "source database file (if master)")
    output_file := flag.String("target", "target.db", "target database file (if master)")
    master_address := flag.String("address", getLocalAddress(), "address of the master node")
    master_port := flag.String("port", "3410", "port of the master node")
    my_address := getLocalAddress()
    my_port := flag.String("p", "3410", "port of the current node")
    temp_dir := flag.String("temp", os.TempDir(), "temporary directory for mapreduce")

    if *is_master {
        do_master(*num_map_tasks, *num_reduce_tasks, *input_file, *output_file, my_address, *my_port, *temp_dir)
    } else {
        do_client(*master_address, *master_port, my_address, *my_port, *temp_dir)
    }

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

    // // gather outputs into final target.db file
    // target, err := createDatabase("target.db")
    //
    //    if err != nil {
    //        log.Fatalf("creating target database: %v", err)
    //    }
    //
    // defer target.Close()
    //
    //    for i := 0; i < r; i++ {
    //        err := gatherInto(target, filepath.Join(tempdir, reduceOutputFile(i)))
    //        if err != nil {
    //            log.Fatalf("gathering reduce output %d: %v", i, err)
    //        }
    //    }
}
