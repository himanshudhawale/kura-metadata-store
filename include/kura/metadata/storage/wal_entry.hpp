#pragma once

#include "kura/metadata/raft/log_entry.hpp"

#include <cstdint>

namespace kura::metadata {

struct WalEntry {
    std::uint32_t checksum{};
    LogEntry log_entry;
};

}  // namespace kura::metadata
