package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue[int]()

	for i := range 10 {
		q.Enqueue(i)
	}

	assert.Equal(t, 10, q.Len())

	for i := range 10 {
		assert.Equal(t, q.Dequeue(), i)
	}
}
