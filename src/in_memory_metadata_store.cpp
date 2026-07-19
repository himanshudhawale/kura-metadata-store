#include "kura/metadata/in_memory_metadata_store.hpp"

#include "kura/metadata/core/store_error.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <ranges>
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

constexpr std::int64_t maximum_lease_id =
    std::numeric_limits<std::int64_t>::max() - 1;

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
    const std::int64_t initial_revision,
    StoreLimits limits)
    : limits_(limits),
      revision_(initial_revision),
      compact_revision_(initial_revision) {
    if (initial_revision < 0) {
        throw std::invalid_argument("initial revision must not be negative");
    }
}

InMemoryMetadataStore::InMemoryMetadataStore(StoreLimits limits)
    : InMemoryMetadataStore(0, limits) {}

InMemoryMetadataStore::InMemoryMetadataStore(
    std::vector<KeyValue> initial_values,
    const std::int64_t initial_revision,
    StoreLimits limits)
    : limits_(limits),
      revision_(initial_revision),
      compact_revision_(initial_revision) {
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

InMemoryMetadataStore::InMemoryMetadataStore(
    InMemoryStoreSnapshot snapshot,
    StoreLimits limits)
    : limits_(limits),
      revision_(snapshot.revision),
      compact_revision_(snapshot.revision),
      logical_tick_(snapshot.logical_tick),
      next_lease_id_(snapshot.next_lease_id),
      next_fencing_token_(snapshot.next_fencing_token) {
    if (revision_ < 0 || next_lease_id_ <= 0
        || next_fencing_token_.value == 0) {
        throw std::invalid_argument("invalid snapshot counters");
    }
    if (snapshot.leases.size() > limits_.max_active_leases) {
        throw StoreError(
            StatusCode::quota_exceeded,
            "lease snapshot exceeds active lease limit");
    }

    for (const LeaseRecord& lease : snapshot.leases) {
        if (lease.id.value <= 0 || lease.id.value > maximum_lease_id
            || lease.fencing_token.value == 0
            || lease.granted_ttl.ticks == 0) {
            throw std::invalid_argument("invalid snapshot lease");
        }
        StoredLease stored{
            .fencing_token = lease.fencing_token,
            .granted_ttl = lease.granted_ttl,
            .expiry_tick = lease.expiry_tick};
        for (const ByteSequence& key : lease.attached_keys) {
            validate_key(key);
            if (!stored.attached_keys.insert(key).second) {
                throw std::invalid_argument(
                    "duplicate snapshot lease attachment");
            }
        }
        if (!leases_.emplace(lease.id, std::move(stored)).second) {
            throw std::invalid_argument("duplicate snapshot lease");
        }
    }

    for (auto& value : snapshot.values) {
        validate_key(value.key);
        if (value.version <= 0 || value.create_revision <= 0
            || value.mod_revision < value.create_revision
            || value.mod_revision > revision_ || value.lease_id < 0) {
            throw std::invalid_argument("invalid snapshot key metadata");
        }
        if (!values_.emplace(value.key, value).second) {
            throw std::invalid_argument("duplicate snapshot key");
        }
        if (value.lease_id != 0) {
            const auto lease = leases_.find(LeaseId{value.lease_id});
            if (lease == leases_.end()
                || !lease->second.attached_keys.contains(value.key)) {
                throw std::invalid_argument(
                    "snapshot key attachment is inconsistent");
            }
        }
    }

    for (const auto& [id, lease] : leases_) {
        if (id.value >= next_lease_id_
            || lease.fencing_token.value
                >= next_fencing_token_.value) {
            throw std::invalid_argument(
                "snapshot allocation counters are not ahead of leases");
        }
        for (const ByteSequence& key : lease.attached_keys) {
            const auto value = values_.find(key);
            if (value == values_.end() || value->second.lease_id != id.value) {
                throw std::invalid_argument(
                    "snapshot lease attachment is inconsistent");
            }
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
    std::map<ByteSequence, KeyValue> working_values = values_;
    std::map<LeaseId, StoredLease> working_leases = leases_;
    detach_key(working_leases, previous);
    working_values.erase(key);
    const std::vector<WatchEvent> events{WatchEvent{
        .revision = Revision{mutation_revision},
        .mutation = MutationEvent{
            .type = MutationEventType::erase,
            .current = std::nullopt,
            .previous = previous}}};
    StagedWatchPublication publication =
        stage_watch_publication_locked(events, mutation_revision);
    values_.swap(working_values);
    leases_.swap(working_leases);
    revision_ = mutation_revision;
    commit_watch_publication_locked(publication);
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

    std::vector<LeaseOwnership> verified_ownership;
    verified_ownership.reserve(request.lease_ownership.size());
    if (!request.lease_ownership.empty()) {
        validate_tick_locked(request.lease_tick);
    }
    for (const LeaseOwnership& ownership : request.lease_ownership) {
        if (ownership.id.value <= 0 || ownership.fencing_token.value == 0) {
            throw std::invalid_argument("invalid lease ownership comparison");
        }
        const auto lease = leases_.find(ownership.id);
        const bool matches =
            lease != leases_.end()
            && request.lease_tick < lease->second.expiry_tick
            && lease->second.fencing_token == ownership.fencing_token;
        comparisons_succeeded = comparisons_succeeded && matches;
        if (matches) {
            verified_ownership.push_back(ownership);
        }
    }

    const std::vector<RequestOperation>& operations =
        comparisons_succeeded ? request.success : request.failure;
    std::vector<ByteSequence> put_keys;
    std::vector<KeyRange> delete_ranges;
    bool has_effective_mutation = false;

    for (const RequestOperation& operation : operations) {
        std::visit(
            [this,
             &verified_ownership,
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
                    if (typed_operation.lease_id < 0) {
                        throw std::invalid_argument(
                            "lease ID must not be negative");
                    }
                    if (typed_operation.lease_id != 0
                        && std::ranges::none_of(
                            verified_ownership,
                            [&typed_operation](
                                const LeaseOwnership& ownership) {
                                return ownership.id.value
                                    == typed_operation.lease_id;
                            })) {
                        throw std::invalid_argument(
                            "leased put requires verified ownership");
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
    std::map<LeaseId, StoredLease> working_leases = leases_;
    std::vector<TransactionOperationResult> responses;
    responses.reserve(operations.size());
    std::vector<WatchEvent> events;

    for (const RequestOperation& operation : operations) {
        std::visit(
            [&working,
             &working_leases,
             &responses,
             &events,
             transaction_revision](
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
                        detach_key(working_leases, *previous);
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
                        .lease_id = typed_operation.lease_id};
                    if (typed_operation.lease_id != 0) {
                        attach_key(
                            working_leases,
                            LeaseId{typed_operation.lease_id},
                            typed_operation.key);
                    }
                    const auto [stored, inserted] =
                        working.insert_or_assign(
                            typed_operation.key,
                            current);
                    static_cast<void>(inserted);
                    events.push_back(WatchEvent{
                        .revision = Revision{transaction_revision},
                        .mutation = MutationEvent{
                            .type = MutationEventType::put,
                            .current = stored->second,
                            .previous = previous}});
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
                        const KeyValue erased = iterator->second;
                        detach_key(working_leases, iterator->second);
                        if (typed_operation.return_previous) {
                            previous.push_back(erased);
                        }
                        events.push_back(WatchEvent{
                            .revision = Revision{transaction_revision},
                            .mutation = MutationEvent{
                                .type = MutationEventType::erase,
                                .current = std::nullopt,
                                .previous = erased}});
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

    std::optional<StagedWatchPublication> publication;
    if (has_effective_mutation) {
        publication =
            stage_watch_publication_locked(events, transaction_revision);
    }
    values_.swap(working);
    leases_.swap(working_leases);
    revision_ = transaction_revision;
    if (!request.lease_ownership.empty()) {
        logical_tick_ = request.lease_tick;
    }
    if (publication.has_value()) {
        commit_watch_publication_locked(*publication);
    }
    return {
        .header = ResponseHeader{.revision = transaction_revision},
        .succeeded = comparisons_succeeded,
        .responses = std::move(responses)};
}

LeaseGrantResult InMemoryMetadataStore::grant_lease(
    const LeaseGrantRequest& request) {
    const std::unique_lock lock(mutex_);
    validate_tick_locked(request.tick);
    if (request.ttl.ticks == 0) {
        throw std::invalid_argument("lease TTL must be positive");
    }
    if (request.tick.value
        > std::numeric_limits<std::uint64_t>::max() - request.ttl.ticks) {
        throw std::overflow_error("lease expiry tick exhausted");
    }
    if (next_fencing_token_.value
        == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("fencing token exhausted");
    }
    if (leases_.size() >= limits_.max_active_leases) {
        throw StoreError(
            StatusCode::quota_exceeded,
            "active lease limit reached");
    }

    LeaseId id = request.requested_id;
    if (id.value == 0) {
        if (next_lease_id_ > maximum_lease_id) {
            throw std::overflow_error("lease ID exhausted");
        }
        id.value = next_lease_id_;
    } else if (id.value < 0 || id.value > maximum_lease_id) {
        throw std::invalid_argument("requested lease ID is outside safe range");
    }
    if (leases_.contains(id)) {
        throw std::invalid_argument("requested lease ID is already live");
    }

    StoredLease stored{
        .fencing_token = next_fencing_token_,
        .granted_ttl = request.ttl,
        .expiry_tick =
            LeaseTick{request.tick.value + request.ttl.ticks}};
    const auto [lease, inserted] = leases_.emplace(id, std::move(stored));
    static_cast<void>(inserted);
    if (id.value >= next_lease_id_) {
        next_lease_id_ = id.value + 1;
    }
    ++next_fencing_token_.value;
    logical_tick_ = request.tick;
    return {
        .lease = lease_record_locked(id, lease->second),
        .revision = revision_};
}

LeaseLookupResult InMemoryMetadataStore::keep_alive(
    const LeaseKeepAliveRequest& request) {
    const std::unique_lock lock(mutex_);
    validate_tick_locked(request.tick);
    if (request.id.value <= 0 || request.fencing_token.value == 0) {
        throw std::invalid_argument("invalid keepalive ownership");
    }

    LeaseLookupResult result = lookup_lease_locked(request.id, request.tick);
    if (result.code == LeaseResultCode::ok) {
        auto& lease = leases_.find(request.id)->second;
        if (lease.fencing_token != request.fencing_token) {
            result.code = LeaseResultCode::fencing_token_mismatch;
        } else {
            if (request.tick.value
                > std::numeric_limits<std::uint64_t>::max()
                    - lease.granted_ttl.ticks) {
                throw std::overflow_error("lease expiry tick exhausted");
            }
            lease.expiry_tick =
                LeaseTick{request.tick.value + lease.granted_ttl.ticks};
            result.lease->expiry_tick = lease.expiry_tick;
            result.remaining_ttl = lease.granted_ttl;
        }
    }
    logical_tick_ = request.tick;
    return result;
}

LeaseLookupResult InMemoryMetadataStore::time_to_live(
    const LeaseTimeToLiveRequest& request) const {
    const std::shared_lock lock(mutex_);
    validate_tick_locked(request.tick);
    if (request.id.value <= 0) {
        throw std::invalid_argument("invalid lease ID");
    }
    return lookup_lease_locked(request.id, request.tick);
}

LeaseCleanupResult InMemoryMetadataStore::revoke_lease(
    const LeaseRevokeRequest& request) {
    const std::unique_lock lock(mutex_);
    validate_tick_locked(request.tick);
    if (request.id.value <= 0 || request.fencing_token.value == 0) {
        throw std::invalid_argument("invalid revoke ownership");
    }
    const auto lease = leases_.find(request.id);
    if (lease == leases_.end()) {
        logical_tick_ = request.tick;
        return {
            .code = LeaseResultCode::not_found,
            .revision = revision_};
    }
    if (lease->second.fencing_token != request.fencing_token) {
        logical_tick_ = request.tick;
        return {
            .code = LeaseResultCode::fencing_token_mismatch,
            .revision = revision_};
    }

    LeaseCleanupResult result =
        remove_leases_locked({request.id}, LeaseResultCode::ok);
    logical_tick_ = request.tick;
    return result;
}

LeaseCleanupResult InMemoryMetadataStore::expire_leases(
    const LeaseTick tick) {
    const std::unique_lock lock(mutex_);
    validate_tick_locked(tick);
    std::vector<LeaseId> expired;
    for (const auto& [id, lease] : leases_) {
        if (lease.expiry_tick <= tick) {
            expired.push_back(id);
        }
    }
    LeaseCleanupResult result =
        remove_leases_locked(expired, LeaseResultCode::ok);
    logical_tick_ = tick;
    return result;
}

InMemoryStoreSnapshot InMemoryMetadataStore::snapshot() const {
    const std::shared_lock lock(mutex_);
    InMemoryStoreSnapshot result{
        .revision = revision_,
        .logical_tick = logical_tick_,
        .next_lease_id = next_lease_id_,
        .next_fencing_token = next_fencing_token_};
    result.values.reserve(values_.size());
    for (const auto& [key, value] : values_) {
        static_cast<void>(key);
        result.values.push_back(value);
    }
    result.leases.reserve(leases_.size());
    for (const auto& [id, lease] : leases_) {
        result.leases.push_back(lease_record_locked(id, lease));
    }
    return result;
}

std::int64_t InMemoryMetadataStore::revision() const {
    const std::shared_lock lock(mutex_);
    return revision_;
}

WatchResponse InMemoryMetadataStore::create_watch(
    const WatchRequest& request) {
    const std::unique_lock lock(mutex_);
    if (request.id.value <= 0) {
        throw StoreError(
            StatusCode::invalid_argument,
            "watch ID must be positive");
    }
    if (request.range.start.empty()) {
        throw StoreError(
            StatusCode::invalid_argument,
            "watch key must not be empty");
    }
    if (!request.range.end.empty()
        && request.range.start >= request.range.end) {
        throw StoreError(
            StatusCode::invalid_argument,
            "watch range start must be less than range end");
    }
    if (request.start_revision.value < 0) {
        throw StoreError(
            StatusCode::invalid_argument,
            "watch start revision must not be negative");
    }
    if (watchers_.contains(request.id)) {
        throw StoreError(
            StatusCode::invalid_argument,
            "watch ID is already active");
    }
    if (watchers_.size() >= limits_.max_watchers) {
        throw StoreError(
            StatusCode::quota_exceeded,
            "active watcher limit reached");
    }

    std::int64_t start_revision = request.start_revision.value;
    if (start_revision == 0) {
        if (revision_ == std::numeric_limits<std::int64_t>::max()) {
            throw StoreError(
                StatusCode::future_revision,
                "no revision exists after the current revision");
        }
        start_revision = revision_ + 1;
    }
    if (start_revision <= compact_revision_) {
        throw StoreError(
            StatusCode::compacted,
            "watch start revision has been compacted",
            compact_revision_);
    }
    if (start_revision > revision_
        && (revision_ == std::numeric_limits<std::int64_t>::max()
            || start_revision != revision_ + 1)) {
        throw StoreError(
            StatusCode::future_revision,
            "watch start revision is in the future");
    }

    WatchState state{.request = request};
    state.request.start_revision = Revision{start_revision};
    for (const EventBatch& batch : history_) {
        if (batch.revision < start_revision) {
            continue;
        }
        std::vector<WatchEvent> matching;
        for (const WatchEvent& event : batch.events) {
            if (watch_matches(state.request, event)) {
                matching.push_back(event);
            }
        }
        if (matching.empty() && !state.request.progress_notifications) {
            continue;
        }
        if (state.pending.size()
            >= limits_.max_watch_pending_responses) {
            throw StoreError(
                StatusCode::quota_exceeded,
                "watch replay exceeds pending response limit");
        }
        state.pending.push_back(WatchResponse{
            .header = ResponseHeader{.revision = batch.revision},
            .id = request.id,
            .events = std::move(matching)});
    }

    watchers_.emplace(request.id, std::move(state));
    return {
        .header = ResponseHeader{.revision = revision_},
        .id = request.id};
}

std::optional<WatchResponse> InMemoryMetadataStore::poll_watch(
    const WatchId id) {
    const std::unique_lock lock(mutex_);
    const auto iterator = watchers_.find(id);
    if (iterator == watchers_.end()) {
        throw StoreError(StatusCode::invalid_argument, "unknown watch ID");
    }
    WatchState& state = iterator->second;
    if (state.terminal.has_value()) {
        WatchResponse response = std::move(*state.terminal);
        watchers_.erase(iterator);
        return response;
    }
    if (state.pending.empty()) {
        return std::nullopt;
    }
    WatchResponse response = std::move(state.pending.front());
    state.pending.pop_front();
    return response;
}

void InMemoryMetadataStore::request_watch_progress(const WatchId id) {
    const std::unique_lock lock(mutex_);
    const auto iterator = watchers_.find(id);
    if (iterator == watchers_.end()) {
        throw StoreError(StatusCode::invalid_argument, "unknown watch ID");
    }
    WatchState& state = iterator->second;
    if (state.terminal.has_value()) {
        return;
    }
    if (state.pending.size() >= limits_.max_watch_pending_responses) {
        state.pending.clear();
        state.terminal = WatchResponse{
            .header = ResponseHeader{.revision = revision_},
            .id = id,
            .status = StatusCode::quota_exceeded,
            .cancelled = true};
        return;
    }
    state.pending.push_back(WatchResponse{
        .header = ResponseHeader{.revision = revision_},
        .id = id});
}

WatchResponse InMemoryMetadataStore::cancel_watch(const WatchId id) {
    const std::unique_lock lock(mutex_);
    const auto iterator = watchers_.find(id);
    if (iterator == watchers_.end()) {
        throw StoreError(StatusCode::invalid_argument, "unknown watch ID");
    }
    watchers_.erase(iterator);
    return {
        .header = ResponseHeader{.revision = revision_},
        .id = id,
        .cancelled = true};
}

std::int64_t InMemoryMetadataStore::compact_revision() const {
    const std::shared_lock lock(mutex_);
    return compact_revision_;
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

    std::map<ByteSequence, KeyValue> working_values = values_;
    std::map<LeaseId, StoredLease> working_leases = leases_;
    const auto [stored, inserted] =
        working_values.insert_or_assign(key, current);
    static_cast<void>(inserted);
    if (previous.has_value()) {
        detach_key(working_leases, *previous);
    }
    const std::vector<WatchEvent> events{WatchEvent{
        .revision = Revision{mutation_revision},
        .mutation = MutationEvent{
            .type = MutationEventType::put,
            .current = stored->second,
            .previous = previous}}};
    StagedWatchPublication publication =
        stage_watch_publication_locked(events, mutation_revision);
    values_.swap(working_values);
    leases_.swap(working_leases);
    revision_ = mutation_revision;
    commit_watch_publication_locked(publication);
    return {
        .current = std::move(current),
        .previous = std::move(previous),
        .revision = mutation_revision};
}

InMemoryMetadataStore::StagedWatchPublication
InMemoryMetadataStore::stage_watch_publication_locked(
    const std::vector<WatchEvent>& events,
    const std::int64_t mutation_revision) const {
    StagedWatchPublication publication{
        .history = history_,
        .watchers = watchers_,
        .compact_revision = compact_revision_};
    publication.history.push_back(EventBatch{
        .revision = mutation_revision,
        .events = events});

    for (auto& [id, state] : publication.watchers) {
        if (state.terminal.has_value()) {
            continue;
        }
        std::vector<WatchEvent> matching;
        for (const WatchEvent& event : events) {
            if (watch_matches(state.request, event)) {
                matching.push_back(event);
            }
        }
        if (matching.empty() && !state.request.progress_notifications) {
            continue;
        }
        if (state.pending.size()
            >= limits_.max_watch_pending_responses) {
            state.pending.clear();
            state.terminal = WatchResponse{
                .header = ResponseHeader{.revision = mutation_revision},
                .id = id,
                .status = StatusCode::quota_exceeded,
                .cancelled = true};
            continue;
        }
        state.pending.push_back(WatchResponse{
            .header = ResponseHeader{.revision = mutation_revision},
            .id = id,
            .events = std::move(matching)});
    }

    while (publication.history.size()
           > limits_.max_watch_history_revisions) {
        publication.compact_revision =
            publication.history.front().revision;
        publication.history.pop_front();
    }
    return publication;
}

void InMemoryMetadataStore::commit_watch_publication_locked(
    StagedWatchPublication& publication) noexcept {
    history_.swap(publication.history);
    watchers_.swap(publication.watchers);
    compact_revision_ = publication.compact_revision;
}

bool InMemoryMetadataStore::watch_matches(
    const WatchRequest& request,
    const WatchEvent& event) {
    if (request.filter == WatchFilter::exclude_put
        && event.mutation.type == MutationEventType::put) {
        return false;
    }
    if (request.filter == WatchFilter::exclude_erase
        && event.mutation.type == MutationEventType::erase) {
        return false;
    }
    const KeyValue& value = event.mutation.current.has_value()
        ? *event.mutation.current
        : *event.mutation.previous;
    if (request.range.end.empty()) {
        return value.key == request.range.start;
    }
    return request.range.start <= value.key
        && value.key < request.range.end;
}

LeaseRecord InMemoryMetadataStore::lease_record_locked(
    const LeaseId id,
    const StoredLease& lease) const {
    return {
        .id = id,
        .fencing_token = lease.fencing_token,
        .granted_ttl = lease.granted_ttl,
        .expiry_tick = lease.expiry_tick,
        .attached_keys = std::vector<ByteSequence>(
            lease.attached_keys.begin(),
            lease.attached_keys.end())};
}

LeaseLookupResult InMemoryMetadataStore::lookup_lease_locked(
    const LeaseId id,
    const LeaseTick tick) const {
    const auto lease = leases_.find(id);
    if (lease == leases_.end()) {
        return {
            .code = LeaseResultCode::not_found,
            .revision = revision_};
    }
    const LeaseRecord record = lease_record_locked(id, lease->second);
    if (tick >= lease->second.expiry_tick) {
        return {
            .code = LeaseResultCode::expired,
            .lease = record,
            .revision = revision_};
    }
    return {
        .code = LeaseResultCode::ok,
        .lease = record,
        .remaining_ttl = LeaseDuration{
            lease->second.expiry_tick.value - tick.value},
        .revision = revision_};
}

LeaseCleanupResult InMemoryMetadataStore::remove_leases_locked(
    const std::vector<LeaseId>& ids,
    const LeaseResultCode code) {
    std::vector<KeyValue> deleted;
    for (const LeaseId id : ids) {
        const auto lease = leases_.find(id);
        if (lease == leases_.end()) {
            throw std::logic_error("lease cleanup references missing lease");
        }
        for (const ByteSequence& key : lease->second.attached_keys) {
            const auto value = values_.find(key);
            if (value == values_.end() || value->second.lease_id != id.value) {
                throw std::logic_error("lease attachment index is inconsistent");
            }
            deleted.push_back(value->second);
        }
    }
    std::ranges::sort(
        deleted,
        {},
        [](const KeyValue& value) -> const ByteSequence& {
            return value.key;
        });

    const std::int64_t cleanup_revision =
        ids.empty() ? revision_ : next_revision_locked();
    LeaseCleanupResult result{
        .code = code,
        .leases = ids,
        .deleted_keys = std::move(deleted),
        .revision = cleanup_revision};
    std::map<ByteSequence, KeyValue> working_values = values_;
    std::map<LeaseId, StoredLease> working_leases = leases_;
    for (const KeyValue& value : result.deleted_keys) {
        working_values.erase(value.key);
    }
    for (const LeaseId id : ids) {
        working_leases.erase(id);
    }
    std::optional<StagedWatchPublication> publication;
    if (!ids.empty()) {
        std::vector<WatchEvent> events;
        events.reserve(result.deleted_keys.size());
        for (const KeyValue& value : result.deleted_keys) {
            events.push_back(WatchEvent{
                .revision = Revision{cleanup_revision},
                .mutation = MutationEvent{
                    .type = MutationEventType::erase,
                    .current = std::nullopt,
                    .previous = value}});
        }
        publication =
            stage_watch_publication_locked(events, cleanup_revision);
    }
    values_.swap(working_values);
    leases_.swap(working_leases);
    revision_ = cleanup_revision;
    if (publication.has_value()) {
        commit_watch_publication_locked(*publication);
    }
    return result;
}

void InMemoryMetadataStore::detach_key_locked(const KeyValue& value) {
    detach_key(leases_, value);
}

void InMemoryMetadataStore::detach_key(
    std::map<LeaseId, StoredLease>& leases,
    const KeyValue& value) {
    if (value.lease_id == 0) {
        return;
    }
    const auto lease = leases.find(LeaseId{value.lease_id});
    if (lease == leases.end()
        || lease->second.attached_keys.erase(value.key) != 1) {
        throw std::logic_error("lease attachment index is inconsistent");
    }
}

void InMemoryMetadataStore::attach_key(
    std::map<LeaseId, StoredLease>& leases,
    const LeaseId id,
    const ByteSequence& key) {
    const auto lease = leases.find(id);
    if (lease == leases.end()) {
        throw std::logic_error("verified lease is missing");
    }
    if (!lease->second.attached_keys.insert(key).second) {
        throw std::logic_error("key is already attached to lease");
    }
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

void InMemoryMetadataStore::validate_tick_locked(const LeaseTick tick) const {
    if (tick < logical_tick_) {
        throw std::invalid_argument("lease tick must not move backwards");
    }
}

}  // namespace kura::metadata
