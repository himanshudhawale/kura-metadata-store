#pragma once

#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"
#include "kura/metadata/lease/lease_snapshot.hpp"
#include "kura/metadata/metadata_store.hpp"

#include <cstdint>
#include <map>
#include <set>
#include <shared_mutex>
#include <vector>

namespace kura::metadata {

class InMemoryMetadataStore final : public MetadataStore {
public:
    explicit InMemoryMetadataStore(std::int64_t initial_revision = 0);

    InMemoryMetadataStore(
        std::vector<KeyValue> initial_values,
        std::int64_t initial_revision);

    explicit InMemoryMetadataStore(InMemoryStoreSnapshot snapshot);

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

    [[nodiscard]] LeaseGrantResult grant_lease(
        const LeaseGrantRequest& request) override;

    [[nodiscard]] LeaseLookupResult keep_alive(
        const LeaseKeepAliveRequest& request) override;

    [[nodiscard]] LeaseLookupResult time_to_live(
        const LeaseTimeToLiveRequest& request) const override;

    [[nodiscard]] LeaseCleanupResult revoke_lease(
        const LeaseRevokeRequest& request) override;

    [[nodiscard]] LeaseCleanupResult expire_leases(
        LeaseTick tick) override;

    [[nodiscard]] InMemoryStoreSnapshot snapshot() const;

    [[nodiscard]] std::int64_t revision() const override;

private:
    struct StoredLease {
        FencingToken fencing_token;
        LeaseDuration granted_ttl;
        LeaseTick expiry_tick;
        std::set<ByteSequence> attached_keys;
    };

    [[nodiscard]] PutResult put_locked(
        const ByteSequence& key,
        const ByteSequence& value);

    [[nodiscard]] LeaseRecord lease_record_locked(
        LeaseId id,
        const StoredLease& lease) const;

    [[nodiscard]] LeaseLookupResult lookup_lease_locked(
        LeaseId id,
        LeaseTick tick) const;

    [[nodiscard]] LeaseCleanupResult remove_leases_locked(
        const std::vector<LeaseId>& ids,
        LeaseResultCode code);

    void detach_key_locked(const KeyValue& value);
    static void detach_key(
        std::map<LeaseId, StoredLease>& leases,
        const KeyValue& value);
    static void attach_key(
        std::map<LeaseId, StoredLease>& leases,
        LeaseId id,
        const ByteSequence& key);

    [[nodiscard]] std::int64_t next_revision_locked() const;

    static void validate_key(const ByteSequence& key);
    void validate_tick_locked(LeaseTick tick) const;

    mutable std::shared_mutex mutex_;
    std::map<ByteSequence, KeyValue> values_;
    std::map<LeaseId, StoredLease> leases_;
    std::int64_t revision_;
    LeaseTick logical_tick_;
    std::int64_t next_lease_id_{1};
    FencingToken next_fencing_token_{1};
};

}  // namespace kura::metadata
