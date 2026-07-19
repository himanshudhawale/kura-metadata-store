#pragma once

#include <cstddef>

namespace kura::metadata {

struct StorageLimits {
    std::size_t wal_segment_bytes{64U << 20U};
    std::size_t snapshot_interval_entries{100'000};
    std::size_t backend_quota_bytes{8ULL << 30U};
};

}  // namespace kura::metadata
