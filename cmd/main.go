package main

import (
    "log"
    "time"
)

func main() {
    // Define node addresses
    grpcAddrs := []string{":50051", ":50052", ":50053"}
    raftAddrs := []string{"127.0.0.1:50061", "127.0.0.1:50062", "127.0.0.1:50063"}
    
    // Initialize leader election for each node
    leaders := make([]*LeaderElection, len(grpcAddrs))
    for i, addr := range raftAddrs {
        leaders[i] = NewLeaderElection(addr, raftAddrs)
        if err := leaders[i].Start(); err != nil {
            log.Fatalf("Failed to start leader election for %s: %v", addr, err)
        }
    }

    // Start multiple nodes
    for i, addr := range grpcAddrs {
        // Create metadata manager for each node
        metadataManager := NewMetadataManager(leaders[i])
        
        // Create and start node server
        node := NewNodeServer(addr, metadataManager)
        go func(addr string) {
            if err := node.Start(); err != nil {
                log.Fatalf("Node at %s failed: %v", addr, err)
            }
        }(addr)
    }

    // Wait for nodes to start and leader election to complete
    time.Sleep(2 * time.Second)

    // Initialize client
    client := NewClient()
    for _, addr := range grpcAddrs {
        if err := client.AddNode(addr); err != nil {
            log.Fatalf("Failed to add node %s: %v", addr, err)
        }
    }

    // Test: Store a file
    fileData := []byte("Hello, this is a test file for FastStore!")
    err := client.StoreFile("testfile", fileData, 10, 2) // Chunk size 10, 2 replicas
    if err != nil {
        log.Fatalf("Failed to store file: %v", err)
    }

    // Test: Retrieve the file
    retrievedData, err := client.GetFile("testfile", 5) // 5 chunks (ceiled from 42/10)
    if err != nil {
        log.Fatalf("Failed to retrieve file: %v", err)
    }
    log.Printf("Retrieved file: %s", string(retrievedData))

    // Keep the program running
    select {}
}

/*
Nodes Setup: Starts three nodes on ports :50051, :50052, and :50053 in separate goroutines.
Client Setup: Initializes a client and connects to the nodes.
Store Operation: Stores a test file ("Hello, this is a test file for FastStore!") by splitting it into chunks of size 10, with 2 replicas per chunk.
Retrieve Operation: Retrieves the file by fetching its chunks, demonstrating the system's functionality.
Chunk Calculation: The file is 42 bytes long, so with a chunk size of 10, it creates 5 chunks (ceiling of 42/10).
*/