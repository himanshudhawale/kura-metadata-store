#pragma once

#include "kura/metadata/raft/persistence.hpp"
#include "kura/metadata/storage/storage_limits.hpp"

#include <filesystem>
#include <functional>
#include <memory>

namespace kura::metadata {

enum class RaftHardStateWriteBoundary {
    temporary_created,
    record_written,
    temporary_synchronized,
    temporary_closed,
    final_replaced,
    directory_synchronized
};

using RaftHardStateFaultHook =
    std::function<void(RaftHardStateWriteBoundary)>;

class RaftHardStateStore {
public:
    virtual ~RaftHardStateStore() = default;

    [[nodiscard]] virtual RaftHardState load() const = 0;
    [[nodiscard]] virtual RaftHardStatePersisted persist(
        const PersistRaftHardState& request) = 0;
};

class FileRaftHardStateStore final : public RaftHardStateStore {
public:
    explicit FileRaftHardStateStore(
        std::filesystem::path directory,
        StorageLimits limits = {},
        RaftHardStateFaultHook fault_hook = {});
    ~FileRaftHardStateStore() override;

    FileRaftHardStateStore(const FileRaftHardStateStore&) = delete;
    FileRaftHardStateStore& operator=(const FileRaftHardStateStore&) = delete;
    FileRaftHardStateStore(FileRaftHardStateStore&&) = delete;
    FileRaftHardStateStore& operator=(FileRaftHardStateStore&&) = delete;

    [[nodiscard]] RaftHardState load() const override;
    [[nodiscard]] RaftHardStatePersisted persist(
        const PersistRaftHardState& request) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kura::metadata
