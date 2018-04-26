package k8stype

type Pod struct {
	ID string // namespace+name
}

type Node struct {
	ID string
}

type Binding struct {
	PodID  string
	NodeID string
}
