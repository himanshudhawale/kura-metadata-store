#pragma once

#include "kura/metadata/storage/snapshot.hpp"

#include <optional>

namespace kura::metadata {

class SnapshotStore {
public:
    virtual ~SnapshotStore() = default;

    virtual void publish(const Snapshot& snapshot) = 0;
    [[nodiscard]] virtual std::optional<Snapshot> latest() const = 0;
};

}  // namespace kura::metadata
