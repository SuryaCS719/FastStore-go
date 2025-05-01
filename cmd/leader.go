package main

import (
    "io"
    "sync"
    "time"

    "github.com/hashicorp/raft"
)

type LeaderElection struct {
    mu      sync.Mutex
    raft    *raft.Raft
    isLeader bool
    peers   []string
    addr    string
}

func NewLeaderElection(addr string, peers []string) *LeaderElection {
    return &LeaderElection{
        peers:   peers,
        addr:    addr,
        isLeader: false,
    }
}

func (l *LeaderElection) Start() error {
    // Initialize Raft configuration
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(l.addr)
    
    // Create Raft transport
    transport, err := raft.NewTCPTransport(l.addr, nil, 3, 10*time.Second, io.Discard)
    if err != nil {
        return err
    }

    // Create Raft store
    store := raft.NewInmemStore()

    // Create Raft snapshot store
    snapshots := raft.NewInmemSnapshotStore()

    // Create Raft instance
    l.raft, err = raft.NewRaft(config, &MetadataFSM{}, store, store, snapshots, transport)
    if err != nil {
        return err
    }

    // Configure initial cluster
    configuration := raft.Configuration{
        Servers: []raft.Server{},
    }
    for _, peer := range l.peers {
        configuration.Servers = append(configuration.Servers, raft.Server{
            ID:      raft.ServerID(peer),
            Address: raft.ServerAddress(peer),
        })
    }
    l.raft.BootstrapCluster(configuration)

    // Start monitoring leadership
    go l.monitorLeadership()

    return nil
}

func (l *LeaderElection) monitorLeadership() {
    for {
        select {
        case <-time.After(100 * time.Millisecond):
            l.mu.Lock()
            l.isLeader = l.raft.State() == raft.Leader
            l.mu.Unlock()
        }
    }
}

func (l *LeaderElection) IsLeader() bool {
    l.mu.Lock()
    defer l.mu.Unlock()
    return l.isLeader
}

// MetadataFSM implements the Raft finite state machine interface
type MetadataFSM struct {
    metadata map[string]string
    mu       sync.Mutex
}

func (m *MetadataFSM) Apply(log *raft.Log) interface{} {
    m.mu.Lock()
    defer m.mu.Unlock()
    // Apply the log entry to the state machine
    // This is where we would update our metadata
    return nil
}

func (m *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    // Create a snapshot of the current state
    return &MetadataSnapshot{metadata: m.metadata}, nil
}

func (m *MetadataFSM) Restore(rc io.ReadCloser) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    // Restore state from a snapshot
    return nil
}

type MetadataSnapshot struct {
    metadata map[string]string
}

func (m *MetadataSnapshot) Persist(sink raft.SnapshotSink) error {
    // Persist the snapshot
    return nil
}

func (m *MetadataSnapshot) Release() {
    // Clean up any resources
} 