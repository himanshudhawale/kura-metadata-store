#pragma once

#include "kura/metadata/storage/durability.hpp"
#include "kura/metadata/storage/storage_limits.hpp"
#include "kura/metadata/storage/wal_entry.hpp"

#include <filesystem>
#include <memory>
#include <span>
#include <vector>

namespace kura::metadata {

class SnapshotStore;

class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    virtual void append(
        std::span<const WalEntry> entries,
        Durability durability) = 0;
    [[nodiscard]] virtual std::vector<WalEntry> recover() = 0;
    virtual void truncate_through(
        LogIndex index,
        const SnapshotStore& snapshots) = 0;
};

class SegmentedWriteAheadLog final : public WriteAheadLog {
public:
    explicit SegmentedWriteAheadLog(
        std::filesystem::path directory,
        StorageLimits limits = {});
    ~SegmentedWriteAheadLog() override;

    SegmentedWriteAheadLog(const SegmentedWriteAheadLog&) = delete;
    SegmentedWriteAheadLog& operator=(const SegmentedWriteAheadLog&) = delete;
    SegmentedWriteAheadLog(SegmentedWriteAheadLog&&) = delete;
    SegmentedWriteAheadLog& operator=(SegmentedWriteAheadLog&&) = delete;

    void append(
        std::span<const WalEntry> entries,
        Durability durability) override;
    [[nodiscard]] std::vector<WalEntry> recover() override;
    void truncate_through(
        LogIndex index,
        const SnapshotStore& snapshots) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kura::metadata
