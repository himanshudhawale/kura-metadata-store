#pragma once

#include "kura/metadata/core/key_range.hpp"
#include "kura/metadata/core/read_consistency.hpp"
#include "kura/metadata/core/revision.hpp"

#include <cstddef>

namespace kura::metadata {

struct RangeRequest {
    KeyRange range;
    Revision revision;
    ReadConsistency consistency{ReadConsistency::linearizable};
    std::size_t limit{};
};

}  // namespace kura::metadata
