package main

import (
	"database/sql"
	"hash/fnv"
)

func (task *MapTask) Process(tempdir string, client Interface) error {
	// Download and open the input file
	sourceURL := makeURL(task.SourceHost, mapSourceFile(task.TaskId))
	db, err := downloadDatabase(sourceURL, tempdir)
	if err != nil {
		return err
	}
	defer db.Close()
	// Create the output files using createDatabase
	outputDBs := make([]*sql.DB, task.NumReduceTasks)
	for r := 0; r < task.NumReduceTasks; r++ {
		outputDB, err := createDatabase(mapOutputFile(task.TaskId, r))
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
	ERR("There was no error, and that is sus")
	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Open the input files
	inputDBs := make([]*sql.DB, task.NumMapTasks)
	for m := 0; m < task.NumMapTasks; m++ {
		db, err := openDatabase(mapOutputFile(m, task.TaskId))
		if err != nil {
			return err
		}
		inputDBs[m] = db
		defer db.Close()
	}
	// Create the output file using createDatabase
	outputDB, err := createDatabase(reduceOutputFile(task.TaskId))
	if err != nil {
		return err
	}
	defer outputDB.Close()

	coco_channels := make([]chan Pair, 0)
	total_rows := 0
	for _, db := range inputDBs {
		rows, err := db.Query("select key, value from pairs order by key, value")
		if err != nil {
			return err
		}
		defer rows.Close()
		var input chan string
		prev_key := ""
		for rows.Next() {
			total_rows++
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return err
			}
			if key != prev_key {
				if input != nil {
					close(input)
				}
				input = make(chan string)
				out_channel := make(chan Pair)
				go client.Reduce(key, input, out_channel)
				prev_key = key
			}
			input <- value
		}
	}
	LOG("Output rows: %d", total_rows)
	total_pairs := 0
	for _, channel := range coco_channels {
		// while channel is open
		for pair := range channel {
			total_pairs++
			outputDB.Exec("insert into pairs (key, value) values (?, ?)", pair.Key, pair.Value)
		}
	}
	LOG("Output pairs: %d", total_pairs)
	ERR("There was no error, and that is sus")
	return nil
}
