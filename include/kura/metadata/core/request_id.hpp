#pragma once

#include <compare>
#include <cstdint>

namespace kura::metadata {

struct RequestId {
    std::uint64_t client{};
    std::uint64_t sequence{};

    auto operator<=>(const RequestId&) const = default;
};

}  // namespace kura::metadata
