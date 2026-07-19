#pragma once

#include <string>
#include <vector>

namespace kura::metadata {

struct HealthCheck {
    std::string name;
    bool healthy{};
    std::string detail;
};

struct HealthStatus {
    bool live{};
    bool ready{};
    std::vector<HealthCheck> checks;
};

}  // namespace kura::metadata
