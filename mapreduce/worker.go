package mapreduce

import (
	"database/sql"
	"hash/fnv"
	"path/filepath"
)

func (task *MapTask) Process(tempdir string, client Interface) error {
	// Download and open the input file
	sourceURL := makeURL(task.SourceHost, mapSourceFile(task.TaskId))
	path := filepath.Join(tempdir, mapInputFile(task.TaskId))
	db, err := downloadDatabase(sourceURL, path)
	if err != nil {
		return err
	}
	defer db.Close()
	// Create the output files using createDatabase
	outputDBs := make([]*sql.DB, task.NumReduceTasks)
	for r := 0; r < task.NumReduceTasks; r++ {
		outputDB, err := createDatabase(filepath.Join(tempdir, mapOutputFile(task.TaskId, r)))
		if err != nil {
			return err
		}
		outputDBs[r] = outputDB
		defer outputDB.Close()
	}

	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		return err
	}
	defer rows.Close()

	coco_channels := make([]chan Pair, 0)
	total_rows := 0
	for rows.Next() {
		total_rows++
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		channel := make(chan Pair)
		coco_channels = append(coco_channels, channel)
		go client.Map(key, value, channel)
	}
	LOG("Input rows: %d", total_rows)

	total_pairs := 0
	for _, channel := range coco_channels {
		// while channel is open
		for pair := range channel {
			total_pairs++
			hash := fnv.New32()
			hash.Write([]byte(pair.Key))
			target := int(hash.Sum32() % uint32(task.NumReduceTasks))
			outputDB := outputDBs[target]
			outputDB.Exec("insert into pairs (key, value) values (?, ?)", pair.Key, pair.Value)
		}
	}
	LOG("Output pairs: %d", total_pairs)
	return nil
}

type Group struct {
	Key   string
	Pairs chan Pair
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Open the input files
	inputdb, err := mergeDatabases(
		task.SourceHosts,
		filepath.Join(tempdir, reduceInputFile(task.TaskId)),
		filepath.Join(tempdir, reduceTempFile(task.TaskId)),
	)
	if err != nil {
		return err
	}
	defer inputdb.Close()

	outputdb, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.TaskId)))
	if err != nil {
		return err
	}
	defer outputdb.Close()

	inputchan := make(chan Pair)
	groupchan := make(chan Group)

	errchan := make(chan error)
	go func() {
		pairs := 0
		for group := range groupchan {
			for val := range group.Pairs {
				pairs += 1
				_, err := outputdb.Exec("insert into pairs (key, value) values (?, ?)", val.Key, val.Value)
				if err != nil {
					ERR("Error inserting into output database: %v", err)
					errchan <- err
					return
				}
			}
		}
		LOG("Output pairs: %d", pairs)
		errchan <- nil
	}()

	go func() {
		lastkey := ""
		keychan := make(chan string)
		for pair := range inputchan {
			if pair.Key != lastkey {
				close(keychan)
				keychan = make(chan string)
				lastkey = pair.Key
				outchan := make(chan Pair)
				groupchan <- Group{pair.Key, outchan}
				go func() {
					err := client.Reduce(pair.Key, keychan, outchan)
					if err != nil {
						errchan <- err
						close(errchan)
						return
					}
				}()
			}
			keychan <- pair.Value
		}
		close(keychan)
		close(groupchan)
		errchan <- nil
	}()

	go func() {
		total_rows := 0
		rows, err := inputdb.Query("select key, value from pairs order by key, value")
		if err != nil {
			errchan <- err
			return
		}
		defer rows.Close()
		for rows.Next() {
			total_rows += 1
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				errchan <- err
				return
			}
			inputchan <- Pair{key, value}
		}
		LOG("Input rows: %d", total_rows)
		close(inputchan)
		errchan <- nil
	}()

	for i := 0; i < 3; i++ {
		if err := <-errchan; err != nil {
			return err
		}
	}

	return nil

	//     inputdb, err := mergeDatabases(task.SourceHosts, filepath.Join(tempdir, filepath.Join(reduceInputFile(task.TaskId))), filepath.Join(tempdir, reduceTempFile(task.TaskId)))
	//     if err != nil {
	//         return err
	//     }
	//     defer inputdb.Close()
	//     // Create the output file using createDatabase
	//     outputDB, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.TaskId)))
	//     if err != nil {
	//         return err
	//     }
	//     defer outputDB.Close()

	//     coco_channels := make(chan chan Pair, 512)
	//     errchan := make(chan error)
	//     go func() {
	//         total_rows := 0
	//         rows, err := inputdb.Query("select key, value from pairs order by key, value")
	//         if err != nil {
	//             close(coco_channels)
	//             errchan <- err
	//             return
	//         }
	//         defer rows.Close()
	//     }()

	//     go func() {
	//         var input chan string
	//         prev_key := ""
	//         for rows.Next() {
	//             total_rows++
	//             var key, value string
	//             if err := rows.Scan(&key, &value); err != nil {
	//                 close(coco_channels)
	//                 errchan <- err
	//                 return
	//             }
	//             if key != prev_key {
	//                 if input != nil {
	//                     close(input)
	//                 }
	//                 input = make(chan string)
	//                 out_channel := make(chan Pair)
	//                 coco_channels <- out_channel
	//                 go client.Reduce(key, input, out_channel)
	//                 prev_key = key
	//             }
	//             input <- value
	//         }
	//         close(input)
	//         LOG("Output rows: %d", total_rows)
	//         close(coco_channels)
	//         errchan <- nil
	//         close(errchan)
	//     }()

	//     total_pairs := 0
	//     for channel := range coco_channels {
	//         ERR("Channel")
	//         // while channel is open
	//         for pair := range channel {
	//             ERR("Pair")
	//             total_pairs++
	//             val_as_int, _ := strconv.Atoi(pair.Value)
	//             outputDB.Exec("insert into pairs (key, value) values (?, ?)", pair.Key, val_as_int)
	//         }
	//     }

	//	if err := <-errchan; err != nil {
	//	    return err
	//	}
	//
	// LOG("Output pairs: %d", total_pairs)
	// ERR("There was no error, and that is sus")
	// return nil
}
