#pragma once

#include "kura/metadata/raft/persistent_state.hpp"

#include <cstdint>

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

}  // namespace kura::metadata
