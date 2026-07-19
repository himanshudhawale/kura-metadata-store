#pragma once

#include <cstdint>
#include <string_view>

namespace kura::metadata {

class Metrics {
public:
    virtual ~Metrics() = default;

    virtual void increment(std::string_view name) = 0;
    virtual void gauge(std::string_view name, std::int64_t value) = 0;
    virtual void observe_seconds(std::string_view name, double value) = 0;
};

}  // namespace kura::metadata
