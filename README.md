# Distributed File System (ADFS)

A robust and scalable distributed storage solution designed to efficiently manage and store critical data across multiple storage nodes. ADFS provides fault tolerance, load balancing, and easy access to large files with built-in redundancy.

## Features

- **Distributed Storage**: Files are split into 32-byte chunks and distributed across multiple storage nodes
- **Fault Tolerance**: Triple replication of chunks ensures data availability even during node failures
- **Load Balancing**: Dynamic chunk allocation prevents single node overload
- **Real-time Health Monitoring**: Heartbeat mechanism for node health tracking
- **Failover & Recovery**: Automatic handling of node failures and recovery
- **Efficient Search**: Distributed word search capability across file chunks

## Architecture

### Components

- **Metadata Server**: Central control server managing file metadata and chunk distribution
- **Storage Nodes**: Distributed nodes storing actual file chunks
- **Heartbeat System**: Continuous health monitoring system

## Key Operations

- File upload with automatic chunk distribution
- File retrieval with load-balanced chunk assembly
- Distributed word search across chunks
- File metadata listing
- Node failover simulation
- Node recovery handling

## Technical Details

### Data Structures

- Chunk management using maps and vectors
- Metadata tracking using unordered maps
- Thread-safe operations with atomic variables and mutexes

### Implementation

- Written in C++
- Uses MPI for inter-process communication
- Thread-safe operations for concurrent access
- 250ms heartbeat interval
- 32-byte chunk size

## Requirements

- C++ compiler
- MPI (e.g., OpenMPI, MPICH)

## Author

Srihari Bandarupalli
