package main

import (
    "github.com/stathat/consistent"
)

type HashRing struct {
    ring *consistent.Consistent
}

func NewHashRing() *HashRing {
    return &HashRing{ring: consistent.New()}
}

func (h *HashRing) AddNode(node string) {
    h.ring.Add(node)
}

func (h *HashRing) GetNodes(chunkID string, numReplicas int) []string {
    nodes := make([]string, 0, numReplicas)
    for i := 0; i < numReplicas; i++ {
        node, _ := h.ring.Get(chunkID + string(rune(i)))
        nodes = append(nodes, node)
    }
    return nodes
}


/*
This code creates a HashRing struct that uses the stathat/consistent library to implement consistent hashing.
NewHashRing() initializes a new hash ring.
AddNode() adds a node (e.g., a server address like ":50051") to the hash ring.
GetNodes() finds numReplicas nodes for a given chunkID, ensuring chunks are distributed across nodes for replication.
*/