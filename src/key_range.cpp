#include "kura/metadata/core/key_range.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace kura::metadata {

std::optional<ByteSequence> prefix_range_end(const ByteSequence& prefix) {
    const auto prefix_bytes = prefix.bytes();
    std::vector<std::uint8_t> end(prefix_bytes.begin(), prefix_bytes.end());

    for (std::size_t size = end.size(); size > 0; --size) {
        auto& byte = end[size - 1];
        if (byte != std::numeric_limits<std::uint8_t>::max()) {
            ++byte;
            end.resize(size);
            return ByteSequence(std::move(end));
        }
    }

    return std::nullopt;
}

}  // namespace kura::metadata
