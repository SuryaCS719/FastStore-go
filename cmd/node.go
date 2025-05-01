package main

import (
    "context"
    "log"
    "net"
    "sync"

    "google.golang.org/grpc"
    "faststore"
)

type NodeServer struct {
    faststore.UnimplementedFastStoreServer
    storage         map[string][]byte
    mu             sync.Mutex
    addr           string
    metadataManager *MetadataManager
}

func NewNodeServer(addr string, metadataManager *MetadataManager) *NodeServer {
    return &NodeServer{
        storage:         make(map[string][]byte),
        addr:           addr,
        metadataManager: metadataManager,
    }
}

func (s *NodeServer) StoreChunk(ctx context.Context, req *faststore.StoreRequest) (*faststore.StoreResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Store the chunk
    s.storage[req.ChunkId] = req.Data

    // Update metadata if this node is the leader
    if s.metadataManager.leaderElection.IsLeader() {
        s.metadataManager.AddChunk(req.ChunkId, req.Nodes)
    }

    return &faststore.StoreResponse{Success: true}, nil
}

func (s *NodeServer) GetChunk(ctx context.Context, req *faststore.GetRequest) (*faststore.GetResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    data, exists := s.storage[req.ChunkId]
    return &faststore.GetResponse{Data: data, Found: exists}, nil
}

func (s *NodeServer) Start() error {
    lis, err := net.Listen("tcp", s.addr)
    if err != nil {
        return err
    }
    server := grpc.NewServer()
    faststore.RegisterFastStoreServer(server, s)
    log.Printf("Node started at %s", s.addr)
    return server.Serve(lis)
}


/*
NodeServer struct: Represents a storage node with an in-memory map (storage) to hold chunks.
NewNodeServer(): Initializes a new node with a given address (e.g., ":50051").
StoreChunk(): Implements the gRPC method to store a chunk, using a mutex (mu) for thread safety.
GetChunk(): Implements the gRPC method to retrieve a chunk, returning whether it was found.
Start(): Starts the gRPC server on the specified address.
*/