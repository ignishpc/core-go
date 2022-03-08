package ipair

type IAbstractPair interface {
	SetFirst(f any)
	GetFirst() any
	SetSecond(f any)
	GetSecond() any
}

type IPair[T1 any, T2 any] struct {
	First  T1
	Second T2
}

func (this *IPair[T1, T2]) SetFirst(f any) {
	this.First = f.(T1)
}

func (this *IPair[T1, T2]) GetFirst() any {
	return this.First
}

func (this *IPair[T1, T2]) SetSecond(f any) {
	this.Second = f.(T2)
}

func (this *IPair[T1, T2]) GetSecond() any {
	return this.Second
}

func New[T1 comparable, T2 comparable](first T1, second T2) IPair[T1, T2] {
	return IPair[T1, T2]{first, second}
}

func NewP[T1 comparable, T2 comparable](first T1, second T2) *IPair[T1, T2] {
	return &IPair[T1, T2]{first, second}
}

func Compare[T1 comparable, T2 comparable](a IPair[T1, T2], b IPair[T1, T2]) bool {
	return a.First == b.First && a.Second == b.Second
}
func CompareP[T1 comparable, T2 comparable](a *IPair[T1, T2], b *IPair[T1, T2]) bool {
	return a.First == b.First && a.Second == b.Second
}
