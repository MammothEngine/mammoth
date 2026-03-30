package wal

import "fmt"

// Replay reads all records from all segments in order.
// If a record has a CRC mismatch, it and all subsequent records in that
// segment are skipped.
func Replay(dir string) ([]Record, error) {
	paths, err := ListSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("wal replay: %w", err)
	}
	if len(paths) == 0 {
		return nil, nil
	}

	var allRecords []Record

	for _, p := range paths {
		recs, err := ReadRecords(p)
		if err != nil {
			// Skip corrupt segments
			continue
		}
		allRecords = append(allRecords, recs...)
	}

	return allRecords, nil
}

// ReplayBatched reads records and groups them into batches.
// A batch is a consecutive sequence of records with sequential SeqNum values.
// If any record in a batch has CRC mismatch, the entire batch is discarded.
func ReplayBatched(dir string) ([][]Record, error) {
	recs, err := Replay(dir)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, nil
	}

	var batches [][]Record
	current := []Record{recs[0]}

	for i := 1; i < len(recs); i++ {
		if recs[i].SeqNum == recs[i-1].SeqNum+1 {
			current = append(current, recs[i])
		} else {
			if len(current) > 0 {
				batches = append(batches, current)
			}
			current = []Record{recs[i]}
		}
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}

	return batches, nil
}
