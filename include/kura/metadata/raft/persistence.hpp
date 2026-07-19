#pragma once

#include "kura/metadata/raft/persistent_state.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct PersistRaftHardState {
    std::uint64_t request_id{};
    RaftHardState state;

    bool operator==(const PersistRaftHardState&) const = default;
};

struct RaftHardStatePersisted {
    std::uint64_t request_id{};
    RaftHardState state;

    bool operator==(const RaftHardStatePersisted&) const = default;
};

struct PersistRaftLog {
    std::uint64_t request_id{};
    std::vector<LogEntry> log;

    bool operator==(const PersistRaftLog&) const = default;
};

struct RaftLogPersisted {
    std::uint64_t request_id{};
    std::vector<LogEntry> log;

    bool operator==(const RaftLogPersisted&) const = default;
};

}  // namespace kura::metadata
