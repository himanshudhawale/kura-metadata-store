#pragma once

#include "kura/metadata/kv/operation_result.hpp"
#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"
#include "kura/metadata/lease/lease_response.hpp"
#include "kura/metadata/watch/watch_request.hpp"
#include "kura/metadata/watch/watch_response.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace kura::metadata {

class InMemoryMetadataStore;

class KuraMetadataBackend {
public:
    virtual ~KuraMetadataBackend() = default;

    [[nodiscard]] virtual LeaseTick current_tick() const = 0;
    [[nodiscard]] virtual StoreRead get(const ByteSequence& key) = 0;
    [[nodiscard]] virtual RangeRead range(
        const ByteSequence& start,
        const ByteSequence& end) = 0;
    [[nodiscard]] virtual TransactionResult transaction(
        TransactionRequest request) = 0;
    [[nodiscard]] virtual LeaseGrantResult grant_lease(
        LeaseDuration ttl) = 0;
    [[nodiscard]] virtual LeaseLookupResult keep_alive(
        LeaseId id,
        FencingToken token) = 0;
    [[nodiscard]] virtual LeaseCleanupResult revoke_lease(
        LeaseId id,
        FencingToken token) = 0;
    [[nodiscard]] virtual WatchResponse create_watch(
        const WatchRequest& request) = 0;
    [[nodiscard]] virtual std::optional<WatchResponse> poll_watch(
        WatchId id) = 0;
    virtual void cancel_watch(WatchId id) noexcept = 0;

    // This primitive is atomic with other operations through this backend.
    [[nodiscard]] virtual bool collect_snapshot_if_unreferenced(
        const ByteSequence& current_key,
        const ByteSequence& snapshot_key,
        const ByteSequence& readers_start,
        const ByteSequence& readers_end,
        std::string_view snapshot_id) = 0;
};

class InProcessKuraMetadataBackend final : public KuraMetadataBackend {
public:
    class TickSource {
    public:
        virtual ~TickSource() = default;
        [[nodiscard]] virtual LeaseTick current_tick() const = 0;
    };

    explicit InProcessKuraMetadataBackend(
        std::shared_ptr<InMemoryMetadataStore> store);
    InProcessKuraMetadataBackend(
        std::shared_ptr<InMemoryMetadataStore> store,
        std::shared_ptr<TickSource> ticks);
    ~InProcessKuraMetadataBackend() override;

    [[nodiscard]] LeaseTick current_tick() const override;
    [[nodiscard]] StoreRead get(const ByteSequence& key) override;
    [[nodiscard]] RangeRead range(
        const ByteSequence& start,
        const ByteSequence& end) override;
    [[nodiscard]] TransactionResult transaction(
        TransactionRequest request) override;
    [[nodiscard]] LeaseGrantResult grant_lease(
        LeaseDuration ttl) override;
    [[nodiscard]] LeaseLookupResult keep_alive(
        LeaseId id,
        FencingToken token) override;
    [[nodiscard]] LeaseCleanupResult revoke_lease(
        LeaseId id,
        FencingToken token) override;
    [[nodiscard]] WatchResponse create_watch(
        const WatchRequest& request) override;
    [[nodiscard]] std::optional<WatchResponse> poll_watch(
        WatchId id) override;
    void cancel_watch(WatchId id) noexcept override;
    [[nodiscard]] bool collect_snapshot_if_unreferenced(
        const ByteSequence& current_key,
        const ByteSequence& snapshot_key,
        const ByteSequence& readers_start,
        const ByteSequence& readers_end,
        std::string_view snapshot_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kura::metadata
