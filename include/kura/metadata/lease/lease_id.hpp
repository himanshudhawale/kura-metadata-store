#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct LeaseId {
    std::int64_t value{};

    auto operator<=>(const LeaseId&) const = default;
};

}  // namespace kura::metadata
