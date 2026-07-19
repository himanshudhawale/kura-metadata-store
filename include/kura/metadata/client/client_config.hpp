#pragma once

#include "kura/metadata/client/endpoint.hpp"
#include "kura/metadata/client/retry_policy.hpp"

#include <chrono>
#include <vector>

namespace kura::metadata {

struct ClientConfig {
    std::vector<Endpoint> endpoints;
    RetryPolicy retry;
    std::chrono::milliseconds deadline{5'000};
};

}  // namespace kura::metadata
