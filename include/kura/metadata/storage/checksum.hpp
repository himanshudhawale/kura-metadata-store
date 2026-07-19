#pragma once

#include <cstdint>
#include <span>

namespace kura::metadata {

class Checksum {
public:
    virtual ~Checksum() = default;

    [[nodiscard]] virtual std::uint32_t compute(
        std::span<const std::uint8_t> bytes) const = 0;
};

}  // namespace kura::metadata
