#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/metadata_store.hpp"

#include <span>

namespace kura::metadata {

class Backend {
public:
    virtual ~Backend() = default;

    virtual void apply(
        Revision revision,
        std::span<const KeyValue> values) = 0;
    [[nodiscard]] virtual RangeRead range(
        const ByteSequence& start,
        const ByteSequence& end,
        Revision revision) const = 0;
};

}  // namespace kura::metadata
