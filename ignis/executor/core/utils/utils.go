package utils

func Min[T Ordered](a, b T) T {
	if a > b {
		return b
	}
	return a
}

func Max[T Ordered](a, b T) T {
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
