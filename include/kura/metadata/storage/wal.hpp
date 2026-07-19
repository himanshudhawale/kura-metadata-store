#pragma once

#include "kura/metadata/storage/durability.hpp"
#include "kura/metadata/storage/wal_entry.hpp"

#include <span>
#include <vector>

namespace kura::metadata {

class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    virtual void append(
        std::span<const WalEntry> entries,
        Durability durability) = 0;
    [[nodiscard]] virtual std::vector<WalEntry> recover() = 0;
    virtual void truncate_through(LogIndex index) = 0;
};

}  // namespace kura::metadata
