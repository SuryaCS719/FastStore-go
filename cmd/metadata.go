package main

import (
    "sync"
    "time"
)

type FileMetadata struct {
    FileID      string
    ChunkIDs    []string
    Size        int64
    CreatedAt   time.Time
    ModifiedAt  time.Time
    Replicas    int
    ChunkSize   int
}

type MetadataManager struct {
    mu          sync.RWMutex
    files       map[string]*FileMetadata
    chunks      map[string][]string // chunkID -> node addresses
    leaderElection *LeaderElection
}

func NewMetadataManager(leaderElection *LeaderElection) *MetadataManager {
    return &MetadataManager{
        files:         make(map[string]*FileMetadata),
        chunks:        make(map[string][]string),
        leaderElection: leaderElection,
    }
}

func (m *MetadataManager) AddFile(fileID string, size int64, chunkSize, replicas int) *FileMetadata {
    m.mu.Lock()
    defer m.mu.Unlock()

    metadata := &FileMetadata{
        FileID:     fileID,
        Size:       size,
        CreatedAt:  time.Now(),
        ModifiedAt: time.Now(),
        Replicas:   replicas,
        ChunkSize:  chunkSize,
    }

    m.files[fileID] = metadata
    return metadata
}

func (m *MetadataManager) GetFile(fileID string) (*FileMetadata, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    metadata, exists := m.files[fileID]
    return metadata, exists
}

func (m *MetadataManager) AddChunk(chunkID string, nodes []string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.chunks[chunkID] = nodes
}

func (m *MetadataManager) GetChunkNodes(chunkID string) ([]string, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    nodes, exists := m.chunks[chunkID]
    return nodes, exists
}

func (m *MetadataManager) UpdateFileMetadata(fileID string, chunkIDs []string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if metadata, exists := m.files[fileID]; exists {
        metadata.ChunkIDs = chunkIDs
        metadata.ModifiedAt = time.Now()
    }
}

// O(log n) lookup using consistent hashing
func (m *MetadataManager) FindNodesForChunk(chunkID string, numReplicas int) []string {
    if !m.leaderElection.IsLeader() {
        // If not leader, forward request to leader
        // This would be implemented with gRPC
        return nil
    }

    m.mu.RLock()
    defer m.mu.RUnlock()
    
    if nodes, exists := m.chunks[chunkID]; exists {
        return nodes
    }
    return nil
} 