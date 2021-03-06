package iio

import "context"

var ctx = context.Background()

const (
	I_VOID      = 0x0
	I_BOOL      = 0x1
	I_I08       = 0x2
	I_I16       = 0x3
	I_I32       = 0x4
	I_I64       = 0x5
	I_DOUBLE    = 0x6
	I_STRING    = 0x7
	I_LIST      = 0x8
	I_SET       = 0x9
	I_MAP       = 0xa
	I_PAIR      = 0xb
	I_BINARY    = 0xc
	I_PAIR_LIST = 0xd
	I_JSON      = 0xe
)
