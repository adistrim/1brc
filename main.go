package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
)

var (
	BufferSize = 32 * 1024 * 1024
	NumWorkers = runtime.NumCPU()
)

type Stats struct {
	Min float64 
	Max float64 
	Sum float64 
	Count int 
}

func main() {
	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, BufferSize)
	chunkChan := make(chan []byte, NumWorkers)
	resultChan := make(chan map[string]Stats, NumWorkers)

	var wg sync.WaitGroup

	// --- merger goroutine ---
	finalStats := make(map[string]Stats)
	var mergeWg sync.WaitGroup
	mergeWg.Add(1)
	go func() {
		defer mergeWg.Done()
		for stats := range resultChan {
			mergeStats(finalStats, stats)
		}
	}()

	// --- workers here lol wokrking working ---
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make(map[string]Stats)
			for chunk := range chunkChan {
				stats := processChunk(chunk)
				mergeStats(local, stats)
			}
			resultChan <- local
		}()
	}

	// --- reader loop ---
	var leftover []byte
	for {
		buf := make([]byte, BufferSize)
		n, err := reader.Read(buf)
		if n == 0 && err != nil {
			break
		}
		chunk := buf[:n]
		if len(leftover) > 0 {
			chunk = append(leftover, chunk...)
			leftover = nil
		}

		lastNewline := bytes.LastIndexByte(chunk, '\n')
		if lastNewline == -1 {
			leftover = append(leftover, chunk...)
			continue
		}

		toSend := chunk[:lastNewline +1]
		leftover = append([]byte{}, chunk[lastNewline+1:]...)

		if len(toSend) > 0 {
			chunkChan <- toSend
		}

		if err != nil {
			break
		}
	}

	if len(leftover) > 0 && bytes.Contains(leftover, []byte(";")) {
		chunkChan <- leftover
	}
	close(chunkChan)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	mergeWg.Wait()

	stations := make([]string, 0, len(finalStats))
	for station := range finalStats {
	    stations = append(stations, station)
	}
	
	sort.Strings(stations)

	for _, station := range stations {
	    stat := finalStats[station]
	    mean := stat.Sum / float64(stat.Count)
	    fmt.Printf("%s;%.1f;%.1f;%.1f\n", station, stat.Min, mean, stat.Max)
	}
}
