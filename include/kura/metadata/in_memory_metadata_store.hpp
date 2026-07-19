#pragma once

#include "kura/metadata/metadata_store.hpp"

#include <cstdint>
#include <map>
#include <shared_mutex>

namespace kura::metadata {

class InMemoryMetadataStore final : public MetadataStore {
public:
    explicit InMemoryMetadataStore(std::int64_t initial_revision = 0);

    [[nodiscard]] StoreRead get(const ByteSequence& key) const override;

    [[nodiscard]] RangeRead range(
        const ByteSequence& start_inclusive,
        const ByteSequence& end_exclusive) const override;

    [[nodiscard]] PutResult put(
        const ByteSequence& key,
        const ByteSequence& value) override;

    [[nodiscard]] DeleteResult erase(const ByteSequence& key) override;

    [[nodiscard]] CompareAndSetResult compare_and_set(
        const ByteSequence& key,
        std::int64_t expected_mod_revision,
        const ByteSequence& new_value) override;

    [[nodiscard]] std::int64_t revision() const override;

private:
    [[nodiscard]] PutResult put_locked(
        const ByteSequence& key,
        const ByteSequence& value);

    [[nodiscard]] std::int64_t next_revision_locked() const;

    static void validate_key(const ByteSequence& key);

    mutable std::shared_mutex mutex_;
    std::map<ByteSequence, KeyValue> values_;
    std::int64_t revision_;
};

}  // namespace kura::metadata
