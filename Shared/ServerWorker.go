package serverUtils

type ServerWorker struct {
	ManagedConnections map[*ManagedConnection]bool
}

func (self *ServerWorker) Accept(v Visitor) {
	v.Visit(self)
}
