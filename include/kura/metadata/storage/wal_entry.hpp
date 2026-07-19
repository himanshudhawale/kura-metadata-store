#pragma once

#include "kura/metadata/raft/log_index.hpp"
#include "kura/metadata/raft/term.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

enum class WalRecordType : std::uint16_t {
    command = 1
};

struct WalEntry {
    WalRecordType type{WalRecordType::command};
    Term term;
    LogIndex index;
    std::vector<std::uint8_t> payload;

    bool operator==(const WalEntry&) const = default;
};

}  // namespace kura::metadata
