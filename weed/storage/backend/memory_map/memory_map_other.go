//go:build !windows

package memory_map

import (
	"errors"
	"os"
)

func (mMap *MemoryMap) CreateMemoryMap(file *os.File, maxLength uint64) {
}

func (mMap *MemoryMap) WriteMemory(offset uint64, length uint64, data []byte) {

}

func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) ([]byte, error) {
	dataSlice := []byte{}

	return dataSlice, errors.New("Memory Map not implemented for this platform")
}

func (mBuffer *MemoryMap) DeleteFileAndMemoryMap() {

}
