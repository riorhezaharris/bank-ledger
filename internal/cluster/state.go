package cluster

import "sync"

// State tracks whether this node can participate in quorum writes.
// canWrite flips to false when the background heartbeat loses contact
// with enough peers to fall below the write quorum (W=2 of N=3).
type State struct {
	nodeID      string
	writeQuorum int

	mu       sync.RWMutex
	canWrite bool
	misses   map[string]int
}

const missThreshold = 3

func NewState(nodeID string, writeQuorum int) *State {
	return &State{
		nodeID:      nodeID,
		writeQuorum: writeQuorum,
		canWrite:    false, // starts false until first successful heartbeat round
		misses:      make(map[string]int),
	}
}

func (s *State) CanWrite() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.canWrite
}

func (s *State) SetCanWrite(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canWrite = v
}

// RecordPing updates the consecutive-miss counter for a peer.
// Returns true if the peer transitioned from dead → alive this tick.
func (s *State) RecordPing(peerAddr string, success bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if success {
		wasDown := s.misses[peerAddr] >= missThreshold
		s.misses[peerAddr] = 0
		return wasDown
	}
	s.misses[peerAddr]++
	return false
}

// ReachableCount returns the number of peers currently considered alive
// (fewer than missThreshold consecutive failures), plus 1 for self.
func (s *State) ReachableCount(peerAddrs []string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 1 // self
	for _, addr := range peerAddrs {
		if s.misses[addr] < missThreshold {
			count++
		}
	}
	return count
}
