package utils

type number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func Min[T number](a, b T) T {
	if a > b {
		return b
	}
	return a
}

func Max[T number](a, b T) T {
	if a < b {
		return b
	}
	return a
}

func Ternary[T any](cond bool, v1 T, v2 T) T {
	if cond {
		return v1
	}
	return v2
}
