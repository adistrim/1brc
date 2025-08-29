package main

import (
	"bytes"
)

func processChunk(chunk []byte) map[string]Stats {
	statsMap := make(map[string]Stats)

	start := 0
	for start < len(chunk) {
		end := bytes.IndexByte(chunk[start:], '\n')
		if end == -1 {
			end = len(chunk) - start
		}
		line := chunk[start : start+end]
		start += end + 1

		if len(line) == 0 {
			continue
		}

		sep := bytes.IndexByte(line, ';')
		if sep == -1 {
			continue
		}

		station := string(line[:sep])
		tempBytes := line[sep+1:]

		val := 0
		neg := false
		for _, c := range tempBytes {
			switch c {
			case '-':
				neg = true
			case '.':
			default:
				val = val*10 + int(c-'0')
			}
		}
		if neg {
			val = -val
		}
		temp := float64(val) / 10.0

		stat := statsMap[station]
		if stat.Count == 0 {
			stat = Stats{Min: temp, Max: temp, Sum: temp, Count: 1}
		} else {
			if temp < stat.Min {
				stat.Min = temp
			}
			if temp > stat.Max {
				stat.Max = temp
			}
			stat.Sum += temp
			stat.Count++
		}
		statsMap[station] = stat
	}

	return statsMap
}

func mergeStats(dst, src map[string]Stats) {
	for station, s := range src {
		if d, exists := dst[station]; exists {
			if s.Min < d.Min {
				d.Min = s.Min
			}
			if s.Max > d.Max {
				d.Max = s.Max
			}
			d.Sum += s.Sum
			d.Count += s.Count
			dst[station] = d
		} else {
			dst[station] = s
		}
	}
}
