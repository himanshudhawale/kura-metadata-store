#pragma once

#include <chrono>

namespace kura::metadata {

class Clock {
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    virtual ~Clock() = default;

    [[nodiscard]] virtual TimePoint now() const = 0;
};

}  // namespace kura::metadata
