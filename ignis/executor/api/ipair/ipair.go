package ipair

type IAbstractPair interface {
	SetFirst(f any)
	GetFirst() any
	SetSecond(f any)
	GetSecond() any
	Copy(first any, second any) IAbstractPair
	New(first any, second any) IAbstractPair
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

func (this *IPair[T1, T2]) Copy(first any, second any) IAbstractPair {
	return &IPair[T1, T2]{this.First, this.Second}
}

func (this *IPair[T1, T2]) New(first any, second any) IAbstractPair {
	return &IPair[T1, T2]{first.(T1), second.(T2)}
}

func New[T1 any, T2 any](first T1, second T2) *IPair[T1, T2] {
	return &IPair[T1, T2]{first, second}
}

func Compare[T1 comparable, T2 comparable](a *IPair[T1, T2], b *IPair[T1, T2]) bool {
	return a.First == b.First && a.Second == b.Second
}
