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

class MetadataStore {
public:
    virtual ~MetadataStore() = default;

    [[nodiscard]] virtual StoreRead get(const ByteSequence& key) const = 0;

    [[nodiscard]] virtual RangeRead range(
        const ByteSequence& start_inclusive,
        const ByteSequence& end_exclusive) const = 0;

    [[nodiscard]] virtual PutResult put(
        const ByteSequence& key,
        const ByteSequence& value) = 0;

    [[nodiscard]] virtual DeleteResult erase(const ByteSequence& key) = 0;

    [[nodiscard]] virtual CompareAndSetResult compare_and_set(
        const ByteSequence& key,
        std::int64_t expected_mod_revision,
        const ByteSequence& new_value) = 0;

    [[nodiscard]] virtual std::int64_t revision() const = 0;
};

}  // namespace kura::metadata
