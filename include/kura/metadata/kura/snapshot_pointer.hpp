#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace kura::metadata {

struct SnapshotPointer {
    std::string snapshot_id;
    std::string manifest_uri;
    std::string schema_id;
    std::vector<std::uint8_t> integrity_hash;

    bool operator==(const SnapshotPointer&) const = default;
};

}  // namespace kura::metadata
