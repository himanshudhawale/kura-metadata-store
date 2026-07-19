#pragma once

#include <cstddef>

namespace kura::metadata {

struct StoreLimits {
    std::size_t max_key_bytes{1U << 20U};
    std::size_t max_value_bytes{8U << 20U};
    std::size_t max_transaction_operations{128};
    std::size_t max_watchers{10'000};
};

}  // namespace kura::metadata
