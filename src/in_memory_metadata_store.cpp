#include "kura/metadata/in_memory_metadata_store.hpp"

#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace kura::metadata {

InMemoryMetadataStore::InMemoryMetadataStore(
    const std::int64_t initial_revision)
    : revision_(initial_revision) {
    if (initial_revision < 0) {
        throw std::invalid_argument("initial revision must not be negative");
    }
}

StoreRead InMemoryMetadataStore::get(const ByteSequence& key) const {
    validate_key(key);
    const std::shared_lock lock(mutex_);
    const auto iterator = values_.find(key);
    return {
        .value = iterator == values_.end()
            ? std::nullopt
            : std::optional<KeyValue>(iterator->second),
        .revision = revision_};
}

RangeRead InMemoryMetadataStore::range(
    const ByteSequence& start_inclusive,
    const ByteSequence& end_exclusive) const {
    validate_key(start_inclusive);
    validate_key(end_exclusive);
    if (start_inclusive >= end_exclusive) {
        throw std::invalid_argument("range start must be less than range end");
    }

    const std::shared_lock lock(mutex_);
    std::vector<KeyValue> result;
    for (auto iterator = values_.lower_bound(start_inclusive);
         iterator != values_.end() && iterator->first < end_exclusive;
         ++iterator) {
        result.push_back(iterator->second);
    }
    return {.values = std::move(result), .revision = revision_};
}

PutResult InMemoryMetadataStore::put(
    const ByteSequence& key,
    const ByteSequence& value) {
    validate_key(key);
    const std::unique_lock lock(mutex_);
    return put_locked(key, value);
}

DeleteResult InMemoryMetadataStore::erase(const ByteSequence& key) {
    validate_key(key);
    const std::unique_lock lock(mutex_);
    const auto iterator = values_.find(key);
    if (iterator == values_.end()) {
        return {
            .deleted = false,
            .previous = std::nullopt,
            .revision = revision_};
    }

    const std::int64_t mutation_revision = next_revision_locked();
    KeyValue previous = iterator->second;
    values_.erase(iterator);
    revision_ = mutation_revision;
    return {
        .deleted = true,
        .previous = std::move(previous),
        .revision = mutation_revision};
}

CompareAndSetResult InMemoryMetadataStore::compare_and_set(
    const ByteSequence& key,
    const std::int64_t expected_mod_revision,
    const ByteSequence& new_value) {
    validate_key(key);
    if (expected_mod_revision < 0) {
        throw std::invalid_argument(
            "expected modification revision must not be negative");
    }
    const std::unique_lock lock(mutex_);
    const auto iterator = values_.find(key);
    const bool matches = iterator == values_.end()
        ? expected_mod_revision == 0
        : iterator->second.mod_revision == expected_mod_revision;
    if (!matches) {
        return {
            .succeeded = false,
            .current = iterator == values_.end()
                ? std::nullopt
                : std::optional<KeyValue>(iterator->second),
            .revision = revision_};
    }

    PutResult result = put_locked(key, new_value);
    return {
        .succeeded = true,
        .current = std::move(result.current),
        .revision = result.revision};
}

std::int64_t InMemoryMetadataStore::revision() const {
    const std::shared_lock lock(mutex_);
    return revision_;
}

PutResult InMemoryMetadataStore::put_locked(
    const ByteSequence& key,
    const ByteSequence& value) {
    const auto iterator = values_.find(key);
    std::optional<KeyValue> previous;
    if (iterator != values_.end()) {
        previous = iterator->second;
    }

    const std::int64_t mutation_revision = next_revision_locked();
    if (previous.has_value()
        && previous->version == std::numeric_limits<std::int64_t>::max()) {
        throw std::overflow_error("key version exhausted");
    }
    const std::int64_t next_version = previous.has_value()
        ? previous->version + 1
        : 1;

    KeyValue current{
        .key = key,
        .value = value,
        .version = next_version,
        .create_revision = previous.has_value()
            ? previous->create_revision
            : mutation_revision,
        .mod_revision = mutation_revision,
        .lease_id = 0};

    const auto [stored, inserted] = values_.insert_or_assign(key, current);
    static_cast<void>(inserted);
    revision_ = mutation_revision;
    return {
        .current = stored->second,
        .previous = std::move(previous),
        .revision = mutation_revision};
}

std::int64_t InMemoryMetadataStore::next_revision_locked() const {
    if (revision_ == std::numeric_limits<std::int64_t>::max()) {
        throw std::overflow_error("store revision exhausted");
    }
    return revision_ + 1;
}

void InMemoryMetadataStore::validate_key(const ByteSequence& key) {
    if (key.empty()) {
        throw std::invalid_argument("key must not be empty");
    }
}

}  // namespace kura::metadata
