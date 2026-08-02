package main

type CommandReceiver interface {
	Accept(Cmd Command) error
}

type Command interface {
	Do(Rcvr CommandReceiver) error
}
