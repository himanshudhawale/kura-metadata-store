#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct Term {
    std::uint64_t value{};

    auto operator<=>(const Term&) const = default;
};

}  // namespace kura::metadata
