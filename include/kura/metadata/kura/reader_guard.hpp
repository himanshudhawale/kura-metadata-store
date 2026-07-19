#pragma once

#include "kura/metadata/kura/snapshot_pointer.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <string>

namespace kura::metadata {

struct ReaderGuard {
    std::string reader_id;
    LeaseId lease;
    SnapshotPointer snapshot;
};

}  // namespace kura::metadata
