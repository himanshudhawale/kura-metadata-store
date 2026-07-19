#pragma once

#include "kura/metadata/raft/append_entries.hpp"
#include "kura/metadata/raft/snapshot_metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace kura::metadata {

using SnapshotPersistenceRequestId = std::uint64_t;

struct RaftSnapshot {
    SnapshotMetadata metadata;
    std::vector<std::uint8_t> state;
    std::uint32_t checksum{};

    bool operator==(const RaftSnapshot&) const = default;
};

struct CreateRaftSnapshot {
    LogIndex applied_index;
    Revision store_revision;
    Revision compaction_revision;
    ClusterMembership membership;
    std::vector<std::uint8_t> state;
};

struct InstallSnapshotRequest {
    Term term;
    NodeId leader;
    std::uint64_t transfer_id{};
    SnapshotMetadata metadata;
    std::uint64_t offset{};
    std::uint64_t total_size{};
    std::vector<std::uint8_t> chunk;
    std::uint32_t state_checksum{};
    bool done{};
    std::optional<ReadIndexContext> read_context;
};

struct InstallSnapshotResponse {
    Term term;
    std::uint64_t transfer_id{};
    bool succeeded{};
    LogIndex last_included_index;
    std::optional<ReadIndexContext> read_context;
};

struct ReceiveInstallSnapshot {
    NodeId from;
    InstallSnapshotRequest request;
};

struct ReceiveInstallSnapshotResponse {
    NodeId from;
    InstallSnapshotResponse response;
};

struct PersistRaftSnapshot {
    SnapshotPersistenceRequestId request_id{};
    RaftSnapshot snapshot;
};

struct RaftSnapshotPersisted {
    SnapshotPersistenceRequestId request_id{};
    RaftSnapshot snapshot;
};

struct RaftSnapshotRestored {
    SnapshotPersistenceRequestId request_id{};
    LogIndex index;
};

struct SendInstallSnapshot {
    NodeId to;
    InstallSnapshotRequest request;
};

struct SendInstallSnapshotResponse {
    NodeId to;
    InstallSnapshotResponse response;
};

struct RestoreStateMachineSnapshot {
    SnapshotPersistenceRequestId request_id{};
    RaftSnapshot snapshot;
};

struct TruncateRaftLogPrefix {
    LogIndex through;
};

struct RaftSnapshotCreated {
    LogIndex index;
};

enum class SnapshotRejection {
    no_applied_entry,
    stale,
    invalid_membership,
    invalid_revision,
    invalid_transfer,
    corrupt,
    too_large,
    persistence_busy
};

struct RaftSnapshotRejected {
    SnapshotRejection reason;
};

}  // namespace kura::metadata
