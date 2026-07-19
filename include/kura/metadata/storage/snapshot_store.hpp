#pragma once

#include "kura/metadata/storage/snapshot.hpp"
#include "kura/metadata/storage/storage_limits.hpp"

#include <filesystem>
#include <memory>
#include <optional>

namespace kura::metadata {

class SnapshotStore {
public:
    virtual ~SnapshotStore() = default;

    virtual void publish(const Snapshot& snapshot) = 0;
    [[nodiscard]] virtual std::optional<Snapshot> latest() const = 0;
};

class FileSnapshotStore final : public SnapshotStore {
public:
    explicit FileSnapshotStore(
        std::filesystem::path directory,
        StorageLimits limits = {});
    ~FileSnapshotStore() override;

    FileSnapshotStore(const FileSnapshotStore&) = delete;
    FileSnapshotStore& operator=(const FileSnapshotStore&) = delete;
    FileSnapshotStore(FileSnapshotStore&&) = delete;
    FileSnapshotStore& operator=(FileSnapshotStore&&) = delete;

    void publish(const Snapshot& snapshot) override;
    [[nodiscard]] std::optional<Snapshot> latest() const override;
    [[nodiscard]] Snapshot validate(
        const std::filesystem::path& path) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kura::metadata
