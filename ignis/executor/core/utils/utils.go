package utils

import "constraints"

func Min[T constraints.Ordered](a, b T) T {
	if a > b {
		return b
	}
	return a
}

func Max[T constraints.Ordered](a, b T) T {
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
