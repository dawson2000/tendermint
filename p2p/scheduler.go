package p2p

// scheduler does QoS scheduling for rawEnvelopes. It enqueues and dequeues
// raw envelopes according to some policy.
type scheduler interface {
	// enqueue returns a channel used to enqueue messages for scheduling.
	enqueue() chan<- rawEnvelope

	// dequeue returns a channel for messages scheduled according to some
	// policy.
	dequeue() <-chan rawEnvelope
}

// fifoScheduler is a simple scheduler that passes messages through in the
// order they were received.
type fifoScheduler struct {
	ch chan rawEnvelope
}

func newFIFOScheduler(bufSize int) *fifoScheduler {
	return &fifoScheduler{ch: make(chan rawEnvelope, bufSize)}
}

func (s *fifoScheduler) dequeue() <-chan rawEnvelope {
	return s.ch
}

func (s *fifoScheduler) enqueue() chan<- rawEnvelope {
	return s.ch
}
