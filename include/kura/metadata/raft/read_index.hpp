#pragma once

#include "kura/metadata/core/request_id.hpp"
#include "kura/metadata/raft/log_index.hpp"

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct ReadIndexContext {
    std::uint64_t value{};

    auto operator<=>(const ReadIndexContext&) const = default;
};

struct ReadIndexRequest {
    RequestId request_id;
};

struct ReadIndexResponse {
    RequestId request_id;
    LogIndex committed_index;
};

enum class ReadIndexFailure {
    not_leader,
    current_term_not_committed,
    capacity_exceeded,
    duplicate_request,
    cancelled,
    timed_out,
    leadership_lost,
    context_exhausted
};

struct ReadIndexRejected {
    RequestId request_id;
    ReadIndexFailure reason;
};

struct CancelReadIndex {
    RequestId request_id;
};

struct TimeoutReadIndex {
    RequestId request_id;
};

}  // namespace kura::metadata
