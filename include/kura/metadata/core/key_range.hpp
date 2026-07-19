#pragma once

#include "kura/metadata/byte_sequence.hpp"

namespace kura::metadata {

struct KeyRange {
    ByteSequence start;
    ByteSequence end;
};

}  // namespace kura::metadata
