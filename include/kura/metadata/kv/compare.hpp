#pragma once

#include "kura/metadata/byte_sequence.hpp"
#include "kura/metadata/kv/compare_result.hpp"
#include "kura/metadata/kv/compare_target.hpp"

#include <cstdint>
#include <variant>

namespace kura::metadata {

struct Compare {
    ByteSequence key;
    CompareTarget target;
    CompareResult result;
    std::variant<std::int64_t, ByteSequence> expected;
};

}  // namespace kura::metadata
