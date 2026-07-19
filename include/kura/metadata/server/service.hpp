#pragma once

#include "kura/metadata/server/health_status.hpp"
#include "kura/metadata/server/server_config.hpp"

namespace kura::metadata {

class Service {
public:
    virtual ~Service() = default;

    virtual void start(const ServerConfig& config) = 0;
    virtual void stop() = 0;
    [[nodiscard]] virtual HealthStatus health() const = 0;
};

}  // namespace kura::metadata
