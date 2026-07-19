#pragma once

#include "kura/metadata/core/clock.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <vector>

namespace kura::metadata {

class LeaseScheduler {
public:
    virtual ~LeaseScheduler() = default;

    [[nodiscard]] virtual std::vector<LeaseId> expired_at(
        Clock::TimePoint now) const = 0;
};

}  // namespace kura::metadata
