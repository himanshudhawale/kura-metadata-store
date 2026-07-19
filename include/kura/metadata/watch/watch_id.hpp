#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct WatchId {
    std::int64_t value{};

    auto operator<=>(const WatchId&) const = default;
};

}  // namespace kura::metadata
