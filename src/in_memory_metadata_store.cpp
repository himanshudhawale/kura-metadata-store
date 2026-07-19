#include "kura/metadata/in_memory_metadata_store.hpp"

#include "kura/metadata/core/store_error.hpp"

#include <algorithm>
#include <chrono>
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
    std::map<ByteSequence, KeyValue> working = values_;
    working.erase(key);
    const std::vector<WatchEvent> events{WatchEvent{
        .revision = Revision{mutation_revision},
        .mutation = MutationEvent{
            .type = MutationEventType::erase,
            .current = std::nullopt,
            .previous = previous}}};
    StagedWatchPublication publication =
        stage_watch_publication_locked(events, mutation_revision);
    values_.swap(working);
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
                    if (typed_operation.lease_id < 0) {
                        throw std::invalid_argument(
                            "lease ID must not be negative");
                    }
                    if (typed_operation.lease_id != 0
                        && !leases_.contains(
                            LeaseId{typed_operation.lease_id})) {
                        throw StoreError(
                            StatusCode::lease_not_found,
                            "transaction references an unknown lease");
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
    std::vector<WatchEvent> events;

    for (const RequestOperation& operation : operations) {
        std::visit(
            [&working, &responses, &events, transaction_revision](
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
                        .lease_id = typed_operation.lease_id};
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
    revision_ = transaction_revision;
    if (publication.has_value()) {
        commit_watch_publication_locked(*publication);
    }
    return {
        .header = ResponseHeader{.revision = transaction_revision},
        .succeeded = comparisons_succeeded,
        .responses = std::move(responses)};
}

LeaseResponse InMemoryMetadataStore::grant_lease(
    const LeaseGrantRequest& request,
    const Clock::TimePoint now) {
    if (request.requested_id.value < 0) {
        throw StoreError(
            StatusCode::invalid_argument,
            "requested lease ID must not be negative");
    }
    if (request.ttl <= std::chrono::seconds::zero()) {
        throw StoreError(
            StatusCode::invalid_argument,
            "lease TTL must be positive");
    }
    if (Clock::TimePoint::max() - now < request.ttl) {
        throw StoreError(
            StatusCode::invalid_argument,
            "lease deadline exceeds the clock range");
    }

    const std::unique_lock lock(mutex_);
    if (leases_.size() >= limits_.max_active_leases) {
        throw StoreError(
            StatusCode::quota_exceeded,
            "active lease limit reached");
    }
    const LeaseId id = request.requested_id.value == 0
        ? next_lease_id_locked()
        : request.requested_id;
    if (leases_.contains(id)) {
        throw StoreError(
            StatusCode::invalid_argument,
            "lease ID is already active");
    }
    leases_.emplace(
        id,
        LeaseState{
            .id = id,
            .granted_ttl = request.ttl,
            .deadline = now + request.ttl});
    return {
        .header = ResponseHeader{.revision = revision_},
        .lease = LeaseRecord{
            .id = id,
            .granted_ttl = request.ttl,
            .remaining_ttl = request.ttl}};
}

LeaseResponse InMemoryMetadataStore::keep_alive(
    const LeaseKeepAliveRequest& request,
    const Clock::TimePoint now) {
    const std::unique_lock lock(mutex_);
    const auto iterator = leases_.find(request.id);
    if (iterator == leases_.end()) {
        throw StoreError(
            StatusCode::lease_not_found,
            "cannot renew an unknown lease");
    }
    LeaseState& lease = iterator->second;
    if (Clock::TimePoint::max() - now < lease.granted_ttl) {
        throw StoreError(
            StatusCode::invalid_argument,
            "lease deadline exceeds the clock range");
    }
    lease.deadline = now + lease.granted_ttl;
    return {
        .header = ResponseHeader{.revision = revision_},
        .lease = LeaseRecord{
            .id = lease.id,
            .granted_ttl = lease.granted_ttl,
            .remaining_ttl = lease.granted_ttl}};
}

LeaseResponse InMemoryMetadataStore::time_to_live(
    const LeaseId id,
    const Clock::TimePoint now) const {
    const std::shared_lock lock(mutex_);
    const auto iterator = leases_.find(id);
    if (iterator == leases_.end()) {
        throw StoreError(
            StatusCode::lease_not_found,
            "unknown lease");
    }
    return {
        .header = ResponseHeader{.revision = revision_},
        .lease = LeaseRecord{
            .id = id,
            .granted_ttl = iterator->second.granted_ttl,
            .remaining_ttl = remaining_ttl(iterator->second, now)}};
}

LeaseResponse InMemoryMetadataStore::revoke_lease(
    const LeaseRevokeRequest& request) {
    const std::unique_lock lock(mutex_);
    const auto iterator = leases_.find(request.id);
    if (iterator == leases_.end()) {
        throw StoreError(
            StatusCode::lease_not_found,
            "cannot revoke an unknown lease");
    }
    return remove_leases_locked(
        {request.id},
        request.id,
        iterator->second.granted_ttl);
}

std::size_t InMemoryMetadataStore::expire_leases(
    const Clock::TimePoint now) {
    const std::unique_lock lock(mutex_);
    std::vector<LeaseId> expired;
    for (const auto& [id, lease] : leases_) {
        if (lease.deadline <= now) {
            expired.push_back(id);
        }
    }
    if (expired.empty()) {
        return 0;
    }
    static_cast<void>(remove_leases_locked(
        expired,
        expired.front(),
        leases_.at(expired.front()).granted_ttl));
    return expired.size();
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

    std::map<ByteSequence, KeyValue> working = values_;
    const auto [stored, inserted] = working.insert_or_assign(key, current);
    static_cast<void>(inserted);
    const std::vector<WatchEvent> events{WatchEvent{
        .revision = Revision{mutation_revision},
        .mutation = MutationEvent{
            .type = MutationEventType::put,
            .current = stored->second,
            .previous = previous}}};
    StagedWatchPublication publication =
        stage_watch_publication_locked(events, mutation_revision);
    values_.swap(working);
    revision_ = mutation_revision;
    commit_watch_publication_locked(publication);
    return {
        .current = std::move(current),
        .previous = std::move(previous),
        .revision = mutation_revision};
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

std::int64_t InMemoryMetadataStore::revision() const {
    const std::shared_lock lock(mutex_);
    return revision_;
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

LeaseResponse InMemoryMetadataStore::remove_leases_locked(
    const std::vector<LeaseId>& ids,
    const LeaseId response_id,
    const std::chrono::seconds granted_ttl) {
    std::map<ByteSequence, KeyValue> working_values = values_;
    std::map<LeaseId, LeaseState> working_leases = leases_;
    std::vector<WatchEvent> events;
    const bool has_attached_keys = std::ranges::any_of(
        values_,
        [&ids](const auto& entry) {
            return std::ranges::find(
                       ids,
                       LeaseId{entry.second.lease_id})
                != ids.end();
        });
    const std::int64_t mutation_revision =
        has_attached_keys ? next_revision_locked() : revision_;

    for (auto iterator = working_values.begin();
         iterator != working_values.end();) {
        if (std::ranges::find(
                ids,
                LeaseId{iterator->second.lease_id})
            == ids.end()) {
            ++iterator;
            continue;
        }
        const KeyValue previous = iterator->second;
        events.push_back(WatchEvent{
            .revision = Revision{mutation_revision},
            .mutation = MutationEvent{
                .type = MutationEventType::erase,
                .current = std::nullopt,
                .previous = previous}});
        iterator = working_values.erase(iterator);
    }
    for (const LeaseId id : ids) {
        working_leases.erase(id);
    }

    std::optional<StagedWatchPublication> publication;
    if (has_attached_keys) {
        publication =
            stage_watch_publication_locked(events, mutation_revision);
    }
    values_.swap(working_values);
    leases_.swap(working_leases);
    revision_ = mutation_revision;
    if (publication.has_value()) {
        commit_watch_publication_locked(*publication);
    }
    return {
        .header = ResponseHeader{.revision = revision_},
        .lease = LeaseRecord{
            .id = response_id,
            .granted_ttl = granted_ttl,
            .remaining_ttl = std::chrono::seconds::zero()}};
}

LeaseId InMemoryMetadataStore::next_lease_id_locked() {
    while (next_lease_id_ > 0
           && leases_.contains(LeaseId{next_lease_id_})) {
        if (next_lease_id_ == std::numeric_limits<std::int64_t>::max()) {
            throw std::overflow_error("lease ID space exhausted");
        }
        ++next_lease_id_;
    }
    if (next_lease_id_ <= 0) {
        throw std::overflow_error("lease ID space exhausted");
    }
    const LeaseId result{next_lease_id_};
    if (next_lease_id_ == std::numeric_limits<std::int64_t>::max()) {
        next_lease_id_ = 0;
    } else {
        ++next_lease_id_;
    }
    return result;
}

std::chrono::seconds InMemoryMetadataStore::remaining_ttl(
    const LeaseState& lease,
    const Clock::TimePoint now) {
    if (now >= lease.deadline) {
        return std::chrono::seconds::zero();
    }
    return std::chrono::ceil<std::chrono::seconds>(
        lease.deadline - now);
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
