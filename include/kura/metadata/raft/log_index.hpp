#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct LogIndex {
    std::uint64_t value{};

    auto operator<=>(const LogIndex&) const = default;
};

}  // namespace kura::metadata
