#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct LeaseId {
    std::int64_t value{};

    auto operator<=>(const LeaseId&) const = default;
};

struct FencingToken {
    std::uint64_t value{};

    auto operator<=>(const FencingToken&) const = default;
};

struct LeaseTick {
    std::uint64_t value{};

    auto operator<=>(const LeaseTick&) const = default;
};

struct LeaseDuration {
    std::uint64_t ticks{};

    auto operator<=>(const LeaseDuration&) const = default;
};

}  // namespace kura::metadata
