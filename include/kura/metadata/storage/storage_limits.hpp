#pragma once

#include <cstddef>
#include <cstdint>

namespace kura::metadata {

struct StorageLimits {
    std::size_t wal_segment_bytes{64U << 20U};
    std::size_t max_wal_payload_bytes{16U << 20U};
    std::size_t max_recovery_records{1'000'000};
    std::size_t max_snapshot_bytes{512U << 20U};
    std::uint32_t max_snapshot_members{10'000};
    std::size_t snapshot_interval_entries{100'000};
    std::size_t backend_quota_bytes{8ULL << 30U};
};

}  // namespace kura::metadata
