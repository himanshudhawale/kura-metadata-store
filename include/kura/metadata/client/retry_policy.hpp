#pragma once

#include <chrono>
#include <cstddef>

namespace kura::metadata {

struct RetryPolicy {
    std::size_t max_attempts{4};
    std::chrono::milliseconds initial_backoff{25};
    std::chrono::milliseconds maximum_backoff{1'000};
};

}  // namespace kura::metadata
