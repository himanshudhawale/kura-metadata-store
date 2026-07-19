#include "kura/metadata/in_memory_metadata_store.hpp"

#include <algorithm>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kura::metadata {
namespace {

void validate_range(const KeyRange& range) {
    if (range.start.empty() || range.end.empty()) {
        throw std::invalid_argument("range bounds must not be empty");
    }
    if (range.start >= range.end) {
        throw std::invalid_argument("range start must be less than range end");
    }
}

bool contains_key(const KeyRange& range, const ByteSequence& key) {
    return range.start <= key && key < range.end;
}

bool ranges_overlap(const KeyRange& left, const KeyRange& right) {
    return left.start < right.end && right.start < left.end;
}

template <typename Value>
bool comparison_matches(
    const Value& actual,
    const Value& expected,
    const CompareResult result) {
    switch (result) {
    case CompareResult::equal:
        return actual == expected;
    case CompareResult::not_equal:
        return actual != expected;
    case CompareResult::greater:
        return actual > expected;
    case CompareResult::less:
        return actual < expected;
    }
    throw std::invalid_argument("unknown comparison result");
}

}  // namespace

InMemoryMetadataStore::InMemoryMetadataStore(
    const std::int64_t initial_revision)
    : revision_(initial_revision) {
    if (initial_revision < 0) {
        throw std::invalid_argument("initial revision must not be negative");
    }
}

InMemoryMetadataStore::InMemoryMetadataStore(
    std::vector<KeyValue> initial_values,
    const std::int64_t initial_revision)
    : revision_(initial_revision) {
    if (initial_revision < 0) {
        throw std::invalid_argument("initial revision must not be negative");
    }
    for (auto& value : initial_values) {
        validate_key(value.key);
        if (value.version <= 0 || value.create_revision <= 0
            || value.mod_revision < value.create_revision
            || value.mod_revision > initial_revision || value.lease_id != 0) {
            throw std::invalid_argument("invalid initial key metadata");
        }
        const auto [iterator, inserted] =
            values_.emplace(value.key, std::move(value));
        static_cast<void>(iterator);
        if (!inserted) {
            throw std::invalid_argument("duplicate initial key");
        }
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

TransactionResult InMemoryMetadataStore::transaction(
    const TransactionRequest& request) {
    const std::unique_lock lock(mutex_);

    bool comparisons_succeeded = true;
    for (const Compare& comparison : request.comparisons) {
        validate_key(comparison.key);
        const auto iterator = values_.find(comparison.key);
        const KeyValue* value =
            iterator == values_.end() ? nullptr : &iterator->second;

        if (comparison.target == CompareTarget::value) {
            const auto* expected =
                std::get_if<ByteSequence>(&comparison.expected);
            if (expected == nullptr) {
                throw std::invalid_argument(
                    "value comparison requires a byte sequence");
            }
            const ByteSequence actual =
                value == nullptr ? ByteSequence{} : value->value;
            const bool matches =
                comparison_matches(actual, *expected, comparison.result);
            comparisons_succeeded = comparisons_succeeded && matches;
            continue;
        }

        const auto* expected =
            std::get_if<std::int64_t>(&comparison.expected);
        if (expected == nullptr) {
            throw std::invalid_argument(
                "numeric comparison requires an int64 value");
        }
        std::int64_t actual = 0;
        switch (comparison.target) {
        case CompareTarget::version:
            actual = value == nullptr ? 0 : value->version;
            break;
        case CompareTarget::create_revision:
            actual = value == nullptr ? 0 : value->create_revision;
            break;
        case CompareTarget::mod_revision:
            actual = value == nullptr ? 0 : value->mod_revision;
            break;
        case CompareTarget::lease_id:
            actual = value == nullptr ? 0 : value->lease_id;
            break;
        case CompareTarget::value:
            throw std::logic_error("value comparison handled separately");
        default:
            throw std::invalid_argument("unknown comparison target");
        }
        const bool matches =
            comparison_matches(actual, *expected, comparison.result);
        comparisons_succeeded = comparisons_succeeded && matches;
    }

    const std::vector<RequestOperation>& operations =
        comparisons_succeeded ? request.success : request.failure;
    std::vector<ByteSequence> put_keys;
    std::vector<KeyRange> delete_ranges;
    bool has_effective_mutation = false;

    for (const RequestOperation& operation : operations) {
        std::visit(
            [this,
             &put_keys,
             &delete_ranges,
             &has_effective_mutation](const auto& typed_operation) {
                using Operation = std::decay_t<decltype(typed_operation)>;
                if constexpr (std::is_same_v<Operation, RangeRequest>) {
                    validate_range(typed_operation.range);
                    if (typed_operation.revision.value != 0) {
                        throw std::invalid_argument(
                            "historical range reads are not implemented");
                    }
                } else if constexpr (std::is_same_v<Operation, PutRequest>) {
                    validate_key(typed_operation.key);
                    if (typed_operation.lease_id != 0) {
                        throw std::invalid_argument(
                            "nonzero lease IDs are not implemented");
                    }
                    if (std::ranges::find(put_keys, typed_operation.key)
                        != put_keys.end()) {
                        throw std::invalid_argument(
                            "transaction writes one key more than once");
                    }
                    if (std::ranges::any_of(
                            delete_ranges,
                            [&typed_operation](const KeyRange& range) {
                                return contains_key(
                                    range,
                                    typed_operation.key);
                            })) {
                        throw std::invalid_argument(
                            "transaction put overlaps a delete");
                    }
                    const auto existing = values_.find(typed_operation.key);
                    if (existing != values_.end()
                        && existing->second.version
                            == std::numeric_limits<std::int64_t>::max()) {
                        throw std::overflow_error("key version exhausted");
                    }
                    put_keys.push_back(typed_operation.key);
                    has_effective_mutation = true;
                } else {
                    validate_range(typed_operation.range);
                    if (std::ranges::any_of(
                            delete_ranges,
                            [&typed_operation](const KeyRange& range) {
                                return ranges_overlap(
                                    range,
                                    typed_operation.range);
                            })) {
                        throw std::invalid_argument(
                            "transaction delete ranges overlap");
                    }
                    if (std::ranges::any_of(
                            put_keys,
                            [&typed_operation](const ByteSequence& key) {
                                return contains_key(
                                    typed_operation.range,
                                    key);
                            })) {
                        throw std::invalid_argument(
                            "transaction delete overlaps a put");
                    }
                    const auto first =
                        values_.lower_bound(typed_operation.range.start);
                    if (first != values_.end()) {
                        has_effective_mutation =
                            has_effective_mutation
                            || first->first < typed_operation.range.end;
                    }
                    delete_ranges.push_back(typed_operation.range);
                }
            },
            operation);
    }

    const std::int64_t transaction_revision =
        has_effective_mutation ? next_revision_locked() : revision_;
    std::map<ByteSequence, KeyValue> working = values_;
    std::vector<TransactionOperationResult> responses;
    responses.reserve(operations.size());

    for (const RequestOperation& operation : operations) {
        std::visit(
            [&working, &responses, transaction_revision](
                const auto& typed_operation) {
                using Operation = std::decay_t<decltype(typed_operation)>;
                if constexpr (std::is_same_v<Operation, RangeRequest>) {
                    std::vector<KeyValue> values;
                    for (auto iterator =
                             working.lower_bound(typed_operation.range.start);
                         iterator != working.end()
                         && iterator->first < typed_operation.range.end;
                         ++iterator) {
                        if (typed_operation.limit != 0
                            && values.size() >= typed_operation.limit) {
                            break;
                        }
                        values.push_back(iterator->second);
                    }
                    responses.emplace_back(RangeRead{
                        .values = std::move(values),
                        .revision = transaction_revision});
                } else if constexpr (std::is_same_v<Operation, PutRequest>) {
                    const auto existing = working.find(typed_operation.key);
                    std::optional<KeyValue> previous;
                    if (existing != working.end()) {
                        previous = existing->second;
                    }
                    const std::int64_t version =
                        previous.has_value() ? previous->version + 1 : 1;
                    KeyValue current{
                        .key = typed_operation.key,
                        .value = typed_operation.value,
                        .version = version,
                        .create_revision = previous.has_value()
                            ? previous->create_revision
                            : transaction_revision,
                        .mod_revision = transaction_revision,
                        .lease_id = 0};
                    const auto [stored, inserted] =
                        working.insert_or_assign(
                            typed_operation.key,
                            current);
                    static_cast<void>(inserted);
                    responses.emplace_back(PutResult{
                        .current = stored->second,
                        .previous = typed_operation.return_previous
                            ? std::move(previous)
                            : std::nullopt,
                        .revision = transaction_revision});
                } else {
                    std::vector<KeyValue> previous;
                    std::size_t deleted = 0;
                    auto iterator =
                        working.lower_bound(typed_operation.range.start);
                    while (iterator != working.end()
                           && iterator->first < typed_operation.range.end) {
                        if (typed_operation.return_previous) {
                            previous.push_back(iterator->second);
                        }
                        iterator = working.erase(iterator);
                        ++deleted;
                    }
                    responses.emplace_back(DeleteRangeResult{
                        .deleted = deleted,
                        .previous = std::move(previous),
                        .revision = transaction_revision});
                }
            },
            operation);
    }

    values_.swap(working);
    revision_ = transaction_revision;
    return {
        .header = ResponseHeader{.revision = transaction_revision},
        .succeeded = comparisons_succeeded,
        .responses = std::move(responses)};
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
