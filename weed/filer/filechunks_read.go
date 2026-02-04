package filer

import (
	"container/list"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func readResolvedChunks(chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *IntervalList[*VisibleInterval]) {
	var points []*Point
	for _, chunk := range chunks {
		if chunk.GetIsChunkManifest() {
			println("This should not happen! A manifest chunk found:", chunk.GetFileIdString())
		}
		start, stop := max(chunk.GetOffset(), startOffset), min(chunk.GetOffset()+int64(chunk.GetSize()), stopOffset)
		if start >= stop {
			continue
		}
		points = append(points, &Point{
			x:       chunk.GetOffset(),
			ts:      chunk.GetModifiedTsNs(),
			chunk:   chunk,
			isStart: true,
		})
		points = append(points, &Point{
			x:       chunk.GetOffset() + int64(chunk.GetSize()),
			ts:      chunk.GetModifiedTsNs(),
			chunk:   chunk,
			isStart: false,
		})
	}
	slices.SortFunc(points, func(a, b *Point) int {
		if a.x != b.x {
			return int(a.x - b.x)
		}
		if a.ts != b.ts {
			return int(a.ts - b.ts)
		}
		if a.isStart {
			return 1
		}
		if b.isStart {
			return -1
		}

		return 0
	})

	var prevX int64
	queue := list.New() // points with higher ts are at the tail
	visibles = NewIntervalList[*VisibleInterval]()
	var prevPoint *Point
	for _, point := range points {
		if queue.Len() > 0 {
			prevPoint = queue.Back().Value.(*Point)
		} else {
			prevPoint = nil
		}
		if point.isStart {
			if prevPoint != nil {
				if point.x != prevX && prevPoint.ts < point.ts {
					addToVisibles(visibles, prevX, prevPoint, point)
					prevX = point.x
				}
			}
			// insert into queue
			if prevPoint == nil || prevPoint.ts < point.ts {
				queue.PushBack(point)
				prevX = point.x
			} else {
				for e := queue.Front(); e != nil; e = e.Next() {
					if e.Value.(*Point).ts > point.ts {
						queue.InsertBefore(point, e)

						break
					}
				}
			}
		} else {
			isLast := true
			for e := queue.Back(); e != nil; e = e.Prev() {
				if e.Value.(*Point).ts == point.ts {
					queue.Remove(e)

					break
				}
				isLast = false
			}
			if isLast && prevPoint != nil {
				addToVisibles(visibles, prevX, prevPoint, point)
				prevX = point.x
			}
		}
	}

	return visibles
}

func addToVisibles(visibles *IntervalList[*VisibleInterval], prevX int64, startPoint *Point, point *Point) {
	if prevX < point.x {
		chunk := startPoint.chunk
		visible := &VisibleInterval{
			start:         prevX,
			stop:          point.x,
			fileId:        chunk.GetFileIdString(),
			modifiedTsNs:  chunk.GetModifiedTsNs(),
			offsetInChunk: prevX - chunk.GetOffset(),
			chunkSize:     chunk.GetSize(),
			cipherKey:     chunk.GetCipherKey(),
			isGzipped:     chunk.GetIsCompressed(),
		}
		appendVisibleInterfal(visibles, visible)
	}
}

func appendVisibleInterfal(visibles *IntervalList[*VisibleInterval], visible *VisibleInterval) {
	visibles.AppendInterval(&Interval[*VisibleInterval]{
		StartOffset: visible.start,
		StopOffset:  visible.stop,
		TsNs:        visible.modifiedTsNs,
		Value:       visible,
	})
}

type Point struct {
	x       int64
	ts      int64
	chunk   *filer_pb.FileChunk
	isStart bool
}
