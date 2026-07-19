#pragma once

#include "kura/metadata/raft/log_entry.hpp"

#include <cstdint>

namespace kura::metadata {

struct ApplyLogEntry {
    std::uint64_t request_id{};
    LogEntry entry;

    bool operator==(const ApplyLogEntry&) const = default;
};

struct LogEntryApplied {
    std::uint64_t request_id{};
    LogIndex index;
};

struct LogEntryApplyFailed {
    std::uint64_t request_id{};
    LogIndex index;
};

struct RetryApplication {};

struct ApplicationBackpressured {
    LogIndex index;
};

}  // namespace kura::metadata
