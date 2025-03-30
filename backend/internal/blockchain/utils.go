package blockchain

// min returns the smaller of x or y
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// max returns the larger of x or y
func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
