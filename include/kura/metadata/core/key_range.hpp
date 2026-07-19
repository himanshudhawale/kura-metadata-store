#pragma once

#include "kura/metadata/byte_sequence.hpp"

#include <optional>

namespace kura::metadata {

struct KeyRange {
    ByteSequence start;
    ByteSequence end;
};

[[nodiscard]] std::optional<ByteSequence> prefix_range_end(
    const ByteSequence& prefix);

}  // namespace kura::metadata
