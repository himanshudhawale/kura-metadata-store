#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct NodeId {
    std::uint64_t value{};

    auto operator<=>(const NodeId&) const = default;
};

}  // namespace kura::metadata
