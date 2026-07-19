#pragma once

#include "kura/metadata/core/limits.hpp"
#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"
#include "kura/metadata/metadata_store.hpp"
#include "kura/metadata/watch/watch_request.hpp"
#include "kura/metadata/watch/watch_response.hpp"

#include <cstdint>
#include <deque>
#include <map>
#include <shared_mutex>
#include <vector>

namespace kura::metadata {

class InMemoryMetadataStore final : public MetadataStore {
public:
    explicit InMemoryMetadataStore(
        std::int64_t initial_revision = 0,
        StoreLimits limits = {});

    explicit InMemoryMetadataStore(StoreLimits limits);

    InMemoryMetadataStore(
        std::vector<KeyValue> initial_values,
        std::int64_t initial_revision,
        StoreLimits limits = {});

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

    [[nodiscard]] TransactionResult transaction(
        const TransactionRequest& request) override;

    [[nodiscard]] WatchResponse create_watch(const WatchRequest& request);

    [[nodiscard]] std::optional<WatchResponse> poll_watch(WatchId id);

    void request_watch_progress(WatchId id);

    [[nodiscard]] WatchResponse cancel_watch(WatchId id);

    [[nodiscard]] std::int64_t compact_revision() const;

    [[nodiscard]] std::int64_t revision() const override;

private:
    struct EventBatch {
        std::int64_t revision;
        std::vector<WatchEvent> events;
    };

    struct WatchState {
        WatchRequest request;
        std::deque<WatchResponse> pending;
        std::optional<WatchResponse> terminal;
    };

    struct StagedWatchPublication {
        std::deque<EventBatch> history;
        std::map<WatchId, WatchState> watchers;
        std::int64_t compact_revision;
    };

    [[nodiscard]] PutResult put_locked(
        const ByteSequence& key,
        const ByteSequence& value);

    [[nodiscard]] StagedWatchPublication stage_watch_publication_locked(
        const std::vector<WatchEvent>& events,
        std::int64_t mutation_revision) const;

    void commit_watch_publication_locked(
        StagedWatchPublication& publication) noexcept;

    static bool watch_matches(
        const WatchRequest& request,
        const WatchEvent& event);

    [[nodiscard]] std::int64_t next_revision_locked() const;

    static void validate_key(const ByteSequence& key);

    mutable std::shared_mutex mutex_;
    std::map<ByteSequence, KeyValue> values_;
    StoreLimits limits_;
    std::int64_t revision_;
    std::int64_t compact_revision_;
    std::deque<EventBatch> history_;
    std::map<WatchId, WatchState> watchers_;
};

}  // namespace kura::metadata
