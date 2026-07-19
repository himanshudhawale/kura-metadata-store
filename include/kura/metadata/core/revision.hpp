#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct Revision {
    std::int64_t value{};

    auto operator<=>(const Revision&) const = default;
};

}  // namespace kura::metadata
