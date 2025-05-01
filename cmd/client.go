package main

import (
    "context"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "faststore"
)

type Client struct {
    hashRing *HashRing
    nodes    map[string]*grpc.ClientConn
}

func NewClient() *Client {
    return &Client{
        hashRing: NewHashRing(),
        nodes:    make(map[string]*grpc.ClientConn),
    }
}

func (c *Client) AddNode(addr string) error {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return err
    }
    c.nodes[addr] = conn
    c.hashRing.AddNode(addr)
    return nil
}

func (c *Client) StoreFile(fileID string, data []byte, chunkSize, numReplicas int) error {
    // Chunk the file
    chunks := [][]byte{}
    for i := 0; i < len(data); i += chunkSize {
        end := i + chunkSize
        if end > len(data) {
            end = len(data)
        }
        chunks = append(chunks, data[i:end])
    }

    // Store each chunk with replication
    for i, chunk := range chunks {
        chunkID := fileID + "_chunk_" + string(rune(i))
        nodes := c.hashRing.GetNodes(chunkID, numReplicas)
        for _, nodeAddr := range nodes {
            conn := c.nodes[nodeAddr]
            client := faststore.NewFastStoreClient(conn)
            _, err := client.StoreChunk(context.Background(), &faststore.StoreRequest{
                ChunkId: chunkID,
                Data:    chunk,
                Nodes:   nodes,
            })
            if err != nil {
                log.Printf("Failed to store chunk %s on %s: %v", chunkID, nodeAddr, err)
                continue
            }
        }
    }
    return nil
}

func (c *Client) GetFile(fileID string, numChunks int) ([]byte, error) {
    var fileData []byte
    for i := 0; i < numChunks; i++ {
        chunkID := fileID + "_chunk_" + string(rune(i))
        nodes := c.hashRing.GetNodes(chunkID, 1) // Try primary node first
        for _, nodeAddr := range nodes {
            conn := c.nodes[nodeAddr]
            client := faststore.NewFastStoreClient(conn)
            resp, err := client.GetChunk(context.Background(), &faststore.GetRequest{ChunkId: chunkID})
            if err == nil && resp.Found {
                fileData = append(fileData, resp.Data...)
                break
            }
            log.Printf("Failed to retrieve chunk %s from %s: %v", chunkID, nodeAddr, err)
        }
    }
    return fileData, nil
}

/*
Client struct: Manages the hash ring and connections to nodes.
NewClient(): Initializes a new client with an empty hash ring and node connections.
AddNode(): Connects to a node via gRPC and adds it to the hash ring.
StoreFile(): Splits a file into chunks, uses the hash ring to find nodes for each chunk, and stores the chunks with replication.
GetFile(): Retrieves chunks by trying the primary node first, demonstrating basic fault tolerance (if a node fails, it logs the failure and continues).
*/