#pragma once

#include "kura/metadata/byte_sequence.hpp"

#include <cstdint>
#include <optional>
#include <vector>

namespace kura::metadata {

struct KeyValue {
    ByteSequence key;
    ByteSequence value;
    std::int64_t version;
    std::int64_t create_revision;
    std::int64_t mod_revision;
    std::int64_t lease_id;

    bool operator==(const KeyValue&) const = default;
};

struct StoreRead {
    std::optional<KeyValue> value;
    std::int64_t revision;
};

struct RangeRead {
    std::vector<KeyValue> values;
    std::int64_t revision;
};

struct PutResult {
    KeyValue current;
    std::optional<KeyValue> previous;
    std::int64_t revision;
};

struct DeleteResult {
    bool deleted;
    std::optional<KeyValue> previous;
    std::int64_t revision;
};

struct CompareAndSetResult {
    bool succeeded;
    std::optional<KeyValue> current;
    std::int64_t revision;
};

}  // namespace kura::metadata
