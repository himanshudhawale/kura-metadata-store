#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kura::metadata {

class ByteSequence {
public:
    ByteSequence() = default;

    explicit ByteSequence(std::vector<std::uint8_t> bytes);

    [[nodiscard]] static ByteSequence copy_from(
        std::span<const std::uint8_t> bytes);

    [[nodiscard]] static ByteSequence from_string(std::string_view value);

    [[nodiscard]] std::span<const std::uint8_t> bytes() const noexcept;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::string to_string() const;

    auto operator<=>(const ByteSequence&) const = default;

private:
    std::vector<std::uint8_t> bytes_;
};

}  // namespace kura::metadata
