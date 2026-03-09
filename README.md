# Storage
Testing and trying storage algos


#Sub project 1

# VolumeForge

A block storage engine built from scratch in C++ — writes data to disk over TCP with crash recovery via a Write-Ahead Log (WAL), and (in progress) replicates across 3 nodes so one can die without losing data.

Think of it as a stripped-down version of what powers OCI Block Volumes or Ceph RBD — built to understand how real storage systems work under the hood.

---

## What it does

- Client sends a `WRITE offset data` or `READ offset len` over TCP
- Server logs the write to `wal.log` and forces it to disk (`fdatasync`) **before** replying OK
- Then writes the data to `data.bin` at the right offset using `pwrite`
- If the server crashes and restarts, it replays the WAL to recover exactly what was committed

---

## Architecture

```
Client ──► Server (:9000)
               ├──► wal.log   (append + fdatasync → crash-safe)
               └──► data.bin  (pwrite at offset)

Crash → restart → replay WAL → truncate WAL → ready
```

**In progress — 3-node replication:**
```
Client ──► Leader ──► Replica1 + Replica2
           Only ACKs after 2 of 3 nodes confirm the write
```
