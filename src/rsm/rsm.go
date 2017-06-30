package rsm

type RSM interface {
	Apply(log interface{}) error
	Snapshot() error
}
