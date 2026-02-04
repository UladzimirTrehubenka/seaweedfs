package storage

import "testing"

func TestBinarySearch(t *testing.T) {
	var testInput = []int{-1, 0, 3, 5, 9, 12}

	if binarySearchForLargerThanTarget(testInput, 4) != 3 {
		t.Errorf("failed to find target %d", 4)
	}
	if binarySearchForLargerThanTarget(testInput, 3) != 3 {
		t.Errorf("failed to find target %d", 3)
	}
	if binarySearchForLargerThanTarget(testInput, 12) != 6 {
		t.Errorf("failed to find target %d", 12)
	}
	if binarySearchForLargerThanTarget(testInput, -1) != 1 {
		t.Errorf("failed to find target %d", -1)
	}
	if binarySearchForLargerThanTarget(testInput, -2) != 0 {
		t.Errorf("failed to find target %d", -2)
	}
}

func binarySearchForLargerThanTarget(nums []int, target int) int {
	l := 0
	h := len(nums)
	for l < h {
		m := (l + h) / 2
		if nums[m] <= target {
			l = m + 1
		} else {
			h = m
		}
	}

	return l
}
