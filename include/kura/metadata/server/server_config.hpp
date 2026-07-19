#pragma once

#include "kura/metadata/core/limits.hpp"
#include "kura/metadata/storage/storage_limits.hpp"

#include <chrono>
#include <string>

namespace kura::metadata {

struct ServerConfig {
    std::string client_address;
    std::string peer_address;
    std::chrono::milliseconds request_timeout{5'000};
    StoreLimits store_limits;
    StorageLimits storage_limits;
};

}  // namespace kura::metadata
