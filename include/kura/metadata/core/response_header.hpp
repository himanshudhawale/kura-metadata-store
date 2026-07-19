#pragma once

#include <cstdint>

namespace kura::metadata {

struct ResponseHeader {
    std::uint64_t cluster_id{};
    std::uint64_t member_id{};
    std::int64_t revision{};
    std::uint64_t raft_term{};
};

}  // namespace kura::metadata
