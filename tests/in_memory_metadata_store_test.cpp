#include "kura/metadata/in_memory_metadata_store.hpp"
#include "kura/metadata/client/client_config.hpp"
#include "kura/metadata/client/metadata_client.hpp"
#include "kura/metadata/client/watch_cursor.hpp"
#include "kura/metadata/core/clock.hpp"
#include "kura/metadata/core/command.hpp"
#include "kura/metadata/core/command_result.hpp"
#include "kura/metadata/core/key_range.hpp"
#include "kura/metadata/core/limits.hpp"
#include "kura/metadata/core/read_consistency.hpp"
#include "kura/metadata/core/response_header.hpp"
#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/core/state_machine.hpp"
#include "kura/metadata/core/store_error.hpp"
#include "kura/metadata/kura/kura_client.hpp"
#include "kura/metadata/kv/compare.hpp"
#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"
#include "kura/metadata/lease/lease_scheduler.hpp"
#include "kura/metadata/raft/raft_node.hpp"
#include "kura/metadata/raft/snapshot_metadata.hpp"
#include "kura/metadata/server/metrics.hpp"
#include "kura/metadata/server/service.hpp"
#include "kura/metadata/storage/backend.hpp"
#include "kura/metadata/storage/checksum.hpp"
#include "kura/metadata/storage/snapshot_store.hpp"
#include "kura/metadata/storage/wal.hpp"
#include "kura/metadata/watch/watch_stream.hpp"

#include <array>
#include <barrier>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace {

using kura::metadata::ByteSequence;
using kura::metadata::Compare;
using kura::metadata::CompareAndSetResult;
using kura::metadata::CompareResult;
using kura::metadata::CompareTarget;
using kura::metadata::DeleteRangeResult;
using kura::metadata::DeleteRequest;
using kura::metadata::InMemoryMetadataStore;
using kura::metadata::InMemoryStoreSnapshot;
using kura::metadata::KeyRange;
using kura::metadata::KeyValue;
using kura::metadata::LeaseDuration;
using kura::metadata::LeaseGrantRequest;
using kura::metadata::LeaseId;
using kura::metadata::LeaseKeepAliveRequest;
using kura::metadata::LeaseOwnership;
using kura::metadata::LeaseResultCode;
using kura::metadata::LeaseRevokeRequest;
using kura::metadata::LeaseTick;
using kura::metadata::LeaseTimeToLiveRequest;
using kura::metadata::FencingToken;
using kura::metadata::PutRequest;
using kura::metadata::PutResult;
using kura::metadata::RangeRead;
using kura::metadata::RangeRequest;
using kura::metadata::RequestOperation;
using kura::metadata::TransactionRequest;
using kura::metadata::prefix_range_end;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename Exception, typename Operation>
void expect_throws(Operation operation, const std::string& message) {
    try {
        operation();
    } catch (const Exception&) {
        return;
    }
    throw TestFailure(message);
}

ByteSequence bytes(const std::string_view value) {
    return ByteSequence::from_string(value);
}

KeyRange key_range(
    const std::string_view start,
    const std::string_view end) {
    return {.start = bytes(start), .end = bytes(end)};
}

PutRequest transaction_put(
    const std::string_view key,
    const std::string_view value,
    const bool return_previous = false,
    const std::int64_t lease_id = 0) {
    return {
        .key = bytes(key),
        .value = bytes(value),
        .lease_id = lease_id,
        .return_previous = return_previous};
}

RangeRequest transaction_range(
    const std::string_view start,
    const std::string_view end,
    const std::size_t limit = 0) {
    return {.range = key_range(start, end), .limit = limit};
}

DeleteRequest transaction_delete(
    const std::string_view start,
    const std::string_view end,
    const bool return_previous = false) {
    return {
        .range = key_range(start, end),
        .return_previous = return_previous};
}

TransactionRequest transaction_request(
    std::vector<Compare> comparisons,
    std::vector<RequestOperation> success,
    std::vector<RequestOperation> failure = {},
    std::vector<LeaseOwnership> lease_ownership = {},
    const LeaseTick lease_tick = {}) {
    return {
        .comparisons = std::move(comparisons),
        .lease_ownership = std::move(lease_ownership),
        .lease_tick = lease_tick,
        .success = std::move(success),
        .failure = std::move(failure)};
}

void byte_sequences_copy_input_and_compare_unsigned() {
    std::array<std::uint8_t, 3> input{1, 2, 3};
    const ByteSequence copied = ByteSequence::copy_from(input);
    input[0] = 9;

    expect(copied.bytes()[0] == 1, "ByteSequence must copy caller memory");
    expect(
        ByteSequence::copy_from(std::array<std::uint8_t, 1>{0xff})
            > ByteSequence::copy_from(std::array<std::uint8_t, 1>{0x01}),
        "ByteSequence ordering must be unsigned");
}

void prefix_range_end_handles_binary_prefixes() {
    expect(
        prefix_range_end(bytes("foo")) == bytes("fop"),
        "ASCII prefix end must increment the final byte");
    expect(
        prefix_range_end(ByteSequence::copy_from(
            std::array<std::uint8_t, 2>{0x01, 0x80}))
            == ByteSequence::copy_from(
                std::array<std::uint8_t, 2>{0x01, 0x81}),
        "multi-byte prefix end must use unsigned byte ordering");
    expect(
        prefix_range_end(ByteSequence::copy_from(
            std::array<std::uint8_t, 4>{0x12, 0xfe, 0xff, 0xff}))
            == ByteSequence::copy_from(
                std::array<std::uint8_t, 2>{0x12, 0xff}),
        "prefix end must carry across trailing 0xff bytes");
}

void prefix_range_end_reports_unbounded_prefixes() {
    expect(
        !prefix_range_end(ByteSequence{}).has_value(),
        "empty prefix must have no finite upper bound");
    expect(
        !prefix_range_end(ByteSequence::copy_from(
            std::array<std::uint8_t, 3>{0xff, 0xff, 0xff}))
             .has_value(),
        "all-0xff prefix must have no finite upper bound");
}

void prefix_range_end_preserves_input() {
    std::array<std::uint8_t, 3> input{0x01, 0xfe, 0xff};
    const ByteSequence prefix = ByteSequence::copy_from(input);

    static_cast<void>(prefix_range_end(prefix));

    expect(
        input == std::array<std::uint8_t, 3>{0x01, 0xfe, 0xff},
        "prefix helper must not mutate caller input");
    expect(
        prefix
            == ByteSequence::copy_from(
                std::array<std::uint8_t, 3>{0x01, 0xfe, 0xff}),
        "prefix helper must not mutate the ByteSequence");
}

void put_creates_and_updates_one_generation() {
    InMemoryMetadataStore store;
    const ByteSequence key = bytes("/kura/table/current");

    const auto created = store.put(key, bytes("snapshot-1"));
    const auto updated = store.put(key, bytes("snapshot-2"));

    expect(created.current.version == 1, "new key version must be one");
    expect(created.current.create_revision == 1, "new key create revision");
    expect(created.current.mod_revision == 1, "new key mod revision");
    expect(!created.previous.has_value(), "new key must not have previous value");
    expect(updated.current.version == 2, "updated key version must increment");
    expect(updated.current.create_revision == 1, "create revision must remain");
    expect(updated.current.mod_revision == 2, "mod revision must advance");
    expect(updated.previous == created.current, "put must return previous value");
    expect(store.revision() == 2, "store revision must advance once per put");
}

void erase_noop_does_not_advance_and_recreate_resets_version() {
    InMemoryMetadataStore store;
    const ByteSequence key = bytes("/kura/table/current");
    static_cast<void>(store.put(key, bytes("snapshot-1")));

    const auto erased = store.erase(key);
    const auto absent = store.erase(key);
    const auto recreated = store.put(key, bytes("snapshot-2"));

    expect(erased.deleted && erased.revision == 2, "existing erase must mutate");
    expect(!absent.deleted && absent.revision == 2, "absent erase is a no-op");
    expect(recreated.current.version == 1, "recreate starts a new generation");
    expect(recreated.current.create_revision == 3, "recreate gets new creation");
}

void compare_and_set_uses_modification_revision() {
    InMemoryMetadataStore store;
    const ByteSequence key = bytes("/kura/table/current");
    const auto initial = store.put(key, bytes("snapshot-1"));

    const auto failed = store.compare_and_set(
        key,
        initial.revision + 1,
        bytes("wrong"));
    const auto succeeded = store.compare_and_set(
        key,
        initial.revision,
        bytes("snapshot-2"));

    expect(!failed.succeeded && failed.revision == 1, "failed CAS is a no-op");
    expect(
        failed.current->value.to_string() == "snapshot-1",
        "failed CAS returns current value");
    expect(succeeded.succeeded && succeeded.revision == 2, "matching CAS wins");
    expect(
        succeeded.current->value.to_string() == "snapshot-2",
        "matching CAS stores new value");
}

void compare_and_set_zero_creates_only_when_absent() {
    InMemoryMetadataStore store;
    const ByteSequence key = bytes("/kura/table/current");

    expect(
        store.compare_and_set(key, 0, bytes("snapshot-1")).succeeded,
        "revision zero must match an absent key");
    expect(
        !store.compare_and_set(key, 0, bytes("snapshot-2")).succeeded,
        "revision zero must not match an existing key");
    expect(store.revision() == 1, "failed create CAS must not advance revision");
}

void range_is_ordered_and_reports_one_revision() {
    InMemoryMetadataStore store;
    static_cast<void>(store.put(bytes("b"), bytes("2")));
    static_cast<void>(store.put(bytes("a"), bytes("1")));
    static_cast<void>(store.put(bytes("c"), bytes("3")));

    const auto result = store.range(bytes("a"), bytes("c"));

    expect(result.revision == 3, "range must report its snapshot revision");
    expect(result.values.size() == 2, "range must be end-exclusive");
    expect(result.values[0].key.to_string() == "a", "range order first key");
    expect(result.values[1].key.to_string() == "b", "range order second key");
}

void invalid_keys_and_ranges_are_rejected() {
    InMemoryMetadataStore store;

    expect_throws<std::invalid_argument>(
        [&store] { static_cast<void>(store.get(ByteSequence{})); },
        "empty keys must be rejected");
    expect_throws<std::invalid_argument>(
        [&store] { static_cast<void>(store.range(bytes("z"), bytes("a"))); },
        "reversed ranges must be rejected");
    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(
                store.compare_and_set(bytes("key"), -1, bytes("value")));
        },
        "negative expected revisions must be rejected");
}

void concurrent_compare_and_set_has_one_winner() {
    InMemoryMetadataStore store;
    const ByteSequence key = bytes("/kura/table/current");
    const auto expected = store.put(key, bytes("snapshot-1")).revision;
    std::barrier start(3);

    auto attempt = [&store, &key, expected, &start](const std::string_view value) {
        start.arrive_and_wait();
        return store.compare_and_set(key, expected, bytes(value));
    };

    auto first = std::async(std::launch::async, attempt, "snapshot-2a");
    auto second = std::async(std::launch::async, attempt, "snapshot-2b");
    start.arrive_and_wait();

    const std::array<CompareAndSetResult, 2> results{
        first.get(),
        second.get()};
    const auto winners =
        static_cast<int>(results[0].succeeded)
        + static_cast<int>(results[1].succeeded);

    expect(winners == 1, "exactly one concurrent CAS must succeed");
    expect(store.revision() == 2, "one CAS mutation advances once");
    expect(store.get(key).value->version == 2, "winning CAS increments version");
}

void revision_exhaustion_does_not_partially_mutate() {
    InMemoryMetadataStore store(
        std::numeric_limits<std::int64_t>::max() - 1);
    const ByteSequence key = bytes("/kura/table/current");
    static_cast<void>(store.put(key, bytes("snapshot-1")));

    expect_throws<std::overflow_error>(
        [&store, &key] { static_cast<void>(store.erase(key)); },
        "erase must reject revision overflow");
    expect(
        store.get(key).value->value.to_string() == "snapshot-1",
        "failed erase must retain current value");

    expect_throws<std::overflow_error>(
        [&store, &key] {
            static_cast<void>(store.put(key, bytes("snapshot-2")));
        },
        "put must reject revision overflow");
    expect(
        store.get(key).value->value.to_string() == "snapshot-1",
        "failed put must retain current value");
}

void transactions_compare_every_target_and_result() {
    InMemoryMetadataStore store;
    static_cast<void>(store.put(bytes("key"), bytes("m")));

    const auto numeric_comparison = [&store](
                                        const CompareTarget target,
                                        const CompareResult result,
                                        const std::int64_t expected) {
        const auto response = store.transaction(transaction_request(
            {Compare{
                .key = bytes("key"),
                .target = target,
                .result = result,
                .expected = expected}},
            {}));
        expect(response.succeeded, "numeric comparison must match");
    };

    for (const CompareTarget target : {
             CompareTarget::version,
             CompareTarget::create_revision,
             CompareTarget::mod_revision}) {
        numeric_comparison(target, CompareResult::equal, 1);
        numeric_comparison(target, CompareResult::not_equal, 2);
        numeric_comparison(target, CompareResult::greater, 0);
        numeric_comparison(target, CompareResult::less, 2);
    }
    numeric_comparison(CompareTarget::lease_id, CompareResult::equal, 0);
    numeric_comparison(CompareTarget::lease_id, CompareResult::not_equal, 1);
    numeric_comparison(CompareTarget::lease_id, CompareResult::greater, -1);
    numeric_comparison(CompareTarget::lease_id, CompareResult::less, 1);

    for (const auto& [result, expected] :
         std::vector<std::pair<CompareResult, ByteSequence>>{
             {CompareResult::equal, bytes("m")},
             {CompareResult::not_equal, bytes("n")},
             {CompareResult::greater, bytes("a")},
             {CompareResult::less, bytes("z")}}) {
        const auto response = store.transaction(transaction_request(
            {Compare{
                .key = bytes("key"),
                .target = CompareTarget::value,
                .result = result,
                .expected = expected}},
            {}));
        expect(response.succeeded, "value comparison must match");
    }

    const auto absent = store.transaction(transaction_request(
        {Compare{
            .key = bytes("absent"),
            .target = CompareTarget::mod_revision,
            .result = CompareResult::equal,
            .expected = std::int64_t{0}}},
        {}));
    expect(absent.succeeded, "absent key metadata must compare as zero");
    expect(store.revision() == 1, "read-only comparisons must not mutate");
}

void transactions_select_success_and_failure_branches() {
    InMemoryMetadataStore store;
    static_cast<void>(store.put(bytes("condition"), bytes("ready")));

    const Compare matches{
        .key = bytes("condition"),
        .target = CompareTarget::value,
        .result = CompareResult::equal,
        .expected = bytes("ready")};
    const auto success = store.transaction(transaction_request(
        {matches},
        {transaction_put("selected", "success")},
        {transaction_put("selected", "failure")}));
    expect(success.succeeded, "matching comparison selects success");
    expect(
        store.get(bytes("selected")).value->value == bytes("success"),
        "success branch must be the only executed branch");

    const auto failure = store.transaction(transaction_request(
        {Compare{
            .key = bytes("condition"),
            .target = CompareTarget::value,
            .result = CompareResult::equal,
            .expected = bytes("not-ready")}},
        {transaction_put("other", "success")},
        {transaction_put("other", "failure")}));
    expect(!failure.succeeded, "failed comparison selects failure");
    expect(
        store.get(bytes("other")).value->value == bytes("failure"),
        "failure branch must be the only executed branch");
    expect(
        failure.header.revision == 3,
        "mutating failure branch advances the revision");

    const auto failed_read_only = store.transaction(transaction_request(
        {Compare{
            .key = bytes("condition"),
            .target = CompareTarget::value,
            .result = CompareResult::equal,
            .expected = bytes("not-ready")}},
        {},
        {transaction_range("a", "z")}));
    expect(
        !failed_read_only.succeeded && failed_read_only.header.revision == 3,
        "read-only failure branch must not advance revision");
}

void transaction_comparisons_are_conjunctive_and_pretransactional() {
    InMemoryMetadataStore store;
    static_cast<void>(store.put(bytes("condition"), bytes("ready")));

    const auto conjunction = store.transaction(transaction_request(
        {
            Compare{
                .key = bytes("condition"),
                .target = CompareTarget::value,
                .result = CompareResult::equal,
                .expected = bytes("ready")},
            Compare{
                .key = bytes("absent"),
                .target = CompareTarget::version,
                .result = CompareResult::equal,
                .expected = std::int64_t{1}},
        },
        {transaction_put("wrong", "branch")},
        {transaction_range("a", "z")}));
    expect(!conjunction.succeeded, "one false comparison fails conjunction");
    expect(!store.get(bytes("wrong")).value, "success branch must not execute");
    expect(store.revision() == 1, "read-only failure branch keeps revision");

    const auto pretransaction = store.transaction(transaction_request(
        {Compare{
            .key = bytes("future"),
            .target = CompareTarget::version,
            .result = CompareResult::equal,
            .expected = std::int64_t{0}}},
        {transaction_put("future", "created")}));
    expect(
        pretransaction.succeeded
            && store.get(bytes("future")).value->version == 1,
        "comparison must observe state before the selected put");
}

void transaction_multi_key_writes_share_one_revision() {
    InMemoryMetadataStore store;
    const auto existing = store.put(bytes("a"), bytes("old"));
    static_cast<void>(store.put(bytes("c"), bytes("remove")));

    const auto result = store.transaction(transaction_request(
        {},
        {
            transaction_put("a", "new", true),
            transaction_put("b", "created"),
            transaction_delete("c", "d", true),
        }));

    expect(result.succeeded, "empty conjunction must select success");
    expect(result.header.revision == 3, "transaction advances exactly once");
    expect(result.responses.size() == 3, "one result per operation");
    const auto& updated = std::get<PutResult>(result.responses[0]);
    const auto& created = std::get<PutResult>(result.responses[1]);
    const auto& deleted = std::get<DeleteRangeResult>(result.responses[2]);
    expect(
        updated.current.mod_revision == 3
            && created.current.mod_revision == 3,
        "all writes must share the transaction revision");
    expect(
        updated.current.version == 2
            && updated.current.create_revision
                == existing.current.create_revision,
        "updates preserve creation and increment version");
    expect(
        created.current.version == 1
            && created.current.create_revision == 3,
        "creates start version one at transaction revision");
    expect(
        updated.previous == existing.current,
        "requested previous put value must be typed");
    expect(
        deleted.deleted == 1 && deleted.previous[0].key == bytes("c")
            && deleted.revision == 3,
        "delete mutation and previous value must share the revision");
    expect(store.revision() == 3, "multi-key branch advances once");
}

void transaction_reads_see_prior_writes_and_use_typed_results() {
    InMemoryMetadataStore store;
    const auto result = store.transaction(transaction_request(
        {},
        {
            transaction_put("a", "1"),
            transaction_range("a", "z", 1),
        }));

    const auto& read = std::get<RangeRead>(result.responses[1]);
    expect(read.values.size() == 1, "range limit must be honored");
    expect(
        read.values[0].key == bytes("a")
            && read.values[0].value == bytes("1"),
        "range must see an earlier transaction put");
    expect(
        read.revision == result.header.revision,
        "read result reports transaction revision");

    const auto read_only = store.transaction(transaction_request(
        {},
        {transaction_range("a", "z")}));
    expect(read_only.header.revision == 1, "read-only branch keeps revision");
    expect(store.revision() == 1, "read-only branch does not advance revision");
}

void absent_transaction_delete_is_a_noop() {
    InMemoryMetadataStore store;
    const auto result = store.transaction(transaction_request(
        {},
        {transaction_delete("missing", "missinh", true)}));

    const auto& deletion = std::get<DeleteRangeResult>(result.responses[0]);
    expect(deletion.deleted == 0, "absent delete reports zero");
    expect(deletion.previous.empty(), "absent delete has no previous values");
    expect(result.header.revision == 0, "absent delete keeps revision");
    expect(store.revision() == 0, "absent delete does not mutate");
}

void duplicate_transaction_writes_are_rejected() {
    InMemoryMetadataStore store;

    const auto expect_invalid = [&store](
                                    std::vector<RequestOperation> operations,
                                    const std::string& message) {
        expect_throws<std::invalid_argument>(
            [&store, &operations] {
                static_cast<void>(store.transaction(
                    transaction_request({}, operations)));
            },
            message);
        expect(store.revision() == 0, "rejected duplicates do not advance");
    };

    expect_invalid(
        {transaction_put("key", "1"), transaction_put("key", "2")},
        "duplicate puts must be rejected");
    expect_invalid(
        {transaction_put("key", "1"), transaction_delete("a", "z")},
        "put/delete conflicts must be rejected");
    expect_invalid(
        {transaction_delete("a", "m"), transaction_delete("k", "z")},
        "overlapping delete ranges must be rejected");
    expect(!store.get(bytes("key")).value, "rejected branch must not write");
}

void invalid_selected_transaction_is_atomic() {
    InMemoryMetadataStore store;

    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(store.transaction(transaction_request(
                {},
                {
                    transaction_put("first", "value"),
                    transaction_range("z", "a"),
                })));
        },
        "late invalid operation must reject the full branch");
    expect(store.revision() == 0, "invalid branch keeps revision");
    expect(!store.get(bytes("first")).value, "invalid branch publishes nothing");

    const auto result = store.transaction(transaction_request(
        {Compare{
            .key = bytes("absent"),
            .target = CompareTarget::version,
            .result = CompareResult::equal,
            .expected = std::int64_t{0}}},
        {transaction_put("valid", "value")},
        {transaction_range("z", "a")}));
    expect(result.succeeded, "unselected invalid branch is not validated");
    expect(store.get(bytes("valid")).value.has_value(), "selected branch runs");
}

void transaction_overflow_is_atomic() {
    InMemoryMetadataStore exhausted_revision(
        std::numeric_limits<std::int64_t>::max());
    expect_throws<std::overflow_error>(
        [&exhausted_revision] {
            static_cast<void>(exhausted_revision.transaction(
                transaction_request(
                    {},
                    {transaction_put("key", "value")})));
        },
        "mutating transaction must reject revision overflow");
    expect(
        exhausted_revision.revision()
            == std::numeric_limits<std::int64_t>::max(),
        "revision overflow must not change revision");
    const auto no_op = exhausted_revision.transaction(transaction_request(
        {},
        {transaction_delete("a", "b")}));
    expect(
        no_op.header.revision == std::numeric_limits<std::int64_t>::max(),
        "no-op transaction at maximum revision succeeds");

    const KeyValue maximum_version{
        .key = bytes("key"),
        .value = bytes("old"),
        .version = std::numeric_limits<std::int64_t>::max(),
        .create_revision = 1,
        .mod_revision = 1,
        .lease_id = 0};
    InMemoryMetadataStore exhausted_version({maximum_version}, 1);
    expect_throws<std::overflow_error>(
        [&exhausted_version] {
            static_cast<void>(exhausted_version.transaction(
                transaction_request(
                    {},
                    {
                        transaction_put("other", "new"),
                        transaction_put("key", "new"),
                    })));
        },
        "key version overflow must reject full transaction");
    expect(
        !exhausted_version.get(bytes("other")).value,
        "version overflow must not publish earlier operations");
    expect(
        exhausted_version.get(bytes("key")).value->value == bytes("old"),
        "version overflow must retain original key");
}

void lease_grant_keepalive_and_ttl_use_logical_ticks() {
    InMemoryMetadataStore store;
    const auto granted = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{},
        .ttl = LeaseDuration{5},
        .tick = LeaseTick{10}});

    expect(granted.lease.id == LeaseId{1}, "auto lease ID starts at one");
    expect(
        granted.lease.fencing_token == FencingToken{1},
        "first grant gets the first fencing token");
    expect(
        granted.lease.expiry_tick == LeaseTick{15},
        "grant deadline is deterministic logical tick plus TTL");
    expect(granted.revision == 0, "grant does not mutate key revision");

    const auto ttl = store.time_to_live(
        LeaseTimeToLiveRequest{.id = LeaseId{1}, .tick = LeaseTick{12}});
    expect(ttl.code == LeaseResultCode::ok, "live lease TTL lookup succeeds");
    expect(ttl.remaining_ttl == LeaseDuration{3}, "TTL reports logical ticks");

    const auto kept = store.keep_alive(LeaseKeepAliveRequest{
        .id = LeaseId{1},
        .fencing_token = FencingToken{1},
        .tick = LeaseTick{13}});
    expect(kept.code == LeaseResultCode::ok, "owner can keep lease alive");
    expect(
        kept.lease->expiry_tick == LeaseTick{18}
            && kept.remaining_ttl == LeaseDuration{5},
        "keepalive resets the full granted TTL");

    const auto wrong_owner = store.keep_alive(LeaseKeepAliveRequest{
        .id = LeaseId{1},
        .fencing_token = FencingToken{2},
        .tick = LeaseTick{14}});
    expect(
        wrong_owner.code == LeaseResultCode::fencing_token_mismatch,
        "wrong fencing token cannot keep a lease alive");
    expect(
        store.time_to_live(
                 LeaseTimeToLiveRequest{
                     .id = LeaseId{1},
                     .tick = LeaseTick{14}})
                .lease->expiry_tick
            == LeaseTick{18},
        "failed keepalive must not change the deadline");
    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(store.keep_alive(LeaseKeepAliveRequest{
                .id = LeaseId{1},
                .fencing_token = FencingToken{1},
                .tick = LeaseTick{13}}));
        },
        "logical command time must never move backwards");
    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(store.grant_lease(LeaseGrantRequest{
                .ttl = LeaseDuration{},
                .tick = LeaseTick{14}}));
        },
        "zero TTL must be rejected");
    const auto empty_expiry = store.expire_leases(LeaseTick{18});
    expect(
        empty_expiry.leases == std::vector<LeaseId>{LeaseId{1}}
            && empty_expiry.deleted_keys.empty()
            && empty_expiry.revision == 1,
        "expiry of an unattached lease still has one lifecycle revision");
}

void lease_revoke_deletes_attached_keys_at_one_revision() {
    InMemoryMetadataStore store;
    const auto granted = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{7},
        .ttl = LeaseDuration{20},
        .tick = LeaseTick{0}});
    const LeaseOwnership ownership{
        .id = granted.lease.id,
        .fencing_token = granted.lease.fencing_token};

    const auto attached = store.transaction(transaction_request(
        {},
        {
            transaction_put("z-key", "z", false, 7),
            transaction_put("a-key", "a", false, 7),
        },
        {},
        {ownership},
        LeaseTick{0}));
    expect(attached.header.revision == 1, "attachment transaction advances once");
    expect(
        store.get(bytes("a-key")).value->lease_id == 7,
        "put stores its lease ID");

    const auto snapshot = store.snapshot();
    expect(
        snapshot.leases[0].attached_keys
            == std::vector<ByteSequence>{bytes("a-key"), bytes("z-key")},
        "snapshot attachment order is deterministic");

    const auto wrong_revoke = store.revoke_lease(LeaseRevokeRequest{
        .id = LeaseId{7},
        .fencing_token = FencingToken{
            ownership.fencing_token.value + 1},
        .tick = LeaseTick{1}});
    expect(
        wrong_revoke.code == LeaseResultCode::fencing_token_mismatch
            && wrong_revoke.revision == 1,
        "stale generation cannot revoke a lease");

    const auto revoked = store.revoke_lease(LeaseRevokeRequest{
        .id = LeaseId{7},
        .fencing_token = ownership.fencing_token,
        .tick = LeaseTick{1}});
    expect(revoked.code == LeaseResultCode::ok, "owner can revoke lease");
    expect(revoked.revision == 2, "revoke cleanup advances exactly once");
    expect(
        revoked.deleted_keys.size() == 2
            && revoked.deleted_keys[0].key == bytes("a-key")
            && revoked.deleted_keys[1].key == bytes("z-key"),
        "revoke returns deterministic key order");
    expect(
        !store.get(bytes("a-key")).value
            && !store.get(bytes("z-key")).value,
        "revoke atomically removes every attached key");
    expect(
        store.time_to_live(
                 LeaseTimeToLiveRequest{
                     .id = LeaseId{7},
                     .tick = LeaseTick{1}})
                .code
            == LeaseResultCode::not_found,
        "revoked lease is removed");
    expect(
        store.revoke_lease(LeaseRevokeRequest{
            .id = LeaseId{7},
            .fencing_token = ownership.fencing_token,
            .tick = LeaseTick{1}})
                .code
            == LeaseResultCode::not_found,
        "unknown revoke has an explicit result");
    expect(store.revision() == 2, "unknown revoke does not advance revision");
}

void paused_owner_is_fenced_after_expiry_and_regrant() {
    InMemoryMetadataStore store;
    const auto old_lease = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{42},
        .ttl = LeaseDuration{3},
        .tick = LeaseTick{0}});
    const LeaseOwnership old_owner{
        .id = LeaseId{42},
        .fencing_token = old_lease.lease.fencing_token};
    static_cast<void>(store.transaction(transaction_request(
        {},
        {transaction_put("owner", "old", false, 42)},
        {},
        {old_owner},
        LeaseTick{0})));

    const auto expired_ttl = store.time_to_live(
        LeaseTimeToLiveRequest{.id = LeaseId{42}, .tick = LeaseTick{3}});
    expect(
        expired_ttl.code == LeaseResultCode::expired,
        "deadline tick classifies the paused owner's lease as expired");
    const auto expired = store.expire_leases(LeaseTick{3});
    expect(
        expired.leases == std::vector<LeaseId>{LeaseId{42}}
            && expired.revision == 2,
        "expiry removes the lease and attached key in one batch");

    const auto new_lease = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{42},
        .ttl = LeaseDuration{3},
        .tick = LeaseTick{3}});
    expect(
        new_lease.lease.fencing_token.value
            > old_lease.lease.fencing_token.value,
        "reusing a lease ID receives a newer fencing token");

    const auto paused_write = store.transaction(transaction_request(
        {},
        {transaction_put("protected", "stale")},
        {transaction_range("a", "z")},
        {old_owner},
        LeaseTick{3}));
    expect(
        !paused_write.succeeded && !store.get(bytes("protected")).value,
        "paused owner cannot write after its generation is fenced");

    const LeaseOwnership new_owner{
        .id = LeaseId{42},
        .fencing_token = new_lease.lease.fencing_token};
    const auto current_write = store.transaction(transaction_request(
        {},
        {transaction_put("protected", "current", false, 42)},
        {},
        {new_owner},
        LeaseTick{3}));
    expect(current_write.succeeded, "current fenced owner can write");
}

void expiry_batches_are_deterministic_after_a_pause() {
    InMemoryMetadataStore store;
    const auto first = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{2},
        .ttl = LeaseDuration{5},
        .tick = LeaseTick{0}});
    const auto second = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{1},
        .ttl = LeaseDuration{5},
        .tick = LeaseTick{0}});
    static_cast<void>(store.transaction(transaction_request(
        {},
        {
            transaction_put("z", "first", false, 2),
            transaction_put("a", "second", false, 1),
        },
        {},
        {
            LeaseOwnership{LeaseId{2}, first.lease.fencing_token},
            LeaseOwnership{LeaseId{1}, second.lease.fencing_token},
        },
        LeaseTick{0})));

    const auto before = store.expire_leases(LeaseTick{4});
    expect(before.leases.empty(), "leases do not expire before deadline");
    expect(before.revision == 1, "empty expiry batch keeps revision");

    const auto after_pause = store.expire_leases(LeaseTick{8});
    expect(
        after_pause.leases
            == std::vector<LeaseId>{LeaseId{1}, LeaseId{2}},
        "overdue leases expire in sorted ID order after a scheduler pause");
    expect(
        after_pause.deleted_keys[0].key == bytes("a")
            && after_pause.deleted_keys[1].key == bytes("z"),
        "expiry deletion order is independent of attachment order");
    expect(after_pause.revision == 2, "expiry batch has one mutation revision");
    expect(store.revision() == 2, "two expired leases do not allocate twice");
}

void reattachment_updates_indexes_and_round_trips_snapshot() {
    InMemoryMetadataStore store;
    const auto short_lease = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{10},
        .ttl = LeaseDuration{5},
        .tick = LeaseTick{0}});
    const auto long_lease = store.grant_lease(LeaseGrantRequest{
        .requested_id = LeaseId{20},
        .ttl = LeaseDuration{10},
        .tick = LeaseTick{0}});
    const LeaseOwnership short_owner{
        LeaseId{10}, short_lease.lease.fencing_token};
    const LeaseOwnership long_owner{
        LeaseId{20}, long_lease.lease.fencing_token};
    static_cast<void>(store.transaction(transaction_request(
        {},
        {transaction_put("key", "short", false, 10)},
        {},
        {short_owner},
        LeaseTick{0})));

    const auto reattached = store.transaction(transaction_request(
        {Compare{
            .key = bytes("key"),
            .target = CompareTarget::lease_id,
            .result = CompareResult::equal,
            .expected = std::int64_t{10}}},
        {transaction_put("key", "long", true, 20)},
        {},
        {long_owner},
        LeaseTick{1}));
    expect(reattached.succeeded, "lease ID comparison guards reattachment");
    expect(
        store.get(bytes("key")).value->lease_id == 20,
        "reattachment stores the new lease");

    const auto saved = store.snapshot();
    InMemoryMetadataStore restored(saved);
    expect(restored.snapshot() == saved, "lease snapshot round trips exactly");

    const auto old_expiry = restored.expire_leases(LeaseTick{5});
    expect(old_expiry.deleted_keys.empty(), "old lease no longer owns key");
    expect(
        restored.get(bytes("key")).value->value == bytes("long"),
        "old lease expiry cannot delete a reattached key");
    const auto new_expiry = restored.expire_leases(LeaseTick{10});
    expect(
        new_expiry.deleted_keys.size() == 1
            && !restored.get(bytes("key")).value,
        "new lease expiry deletes the reattached key");
}

void leased_put_requires_verified_live_ownership() {
    InMemoryMetadataStore store;
    const auto granted = store.grant_lease(LeaseGrantRequest{
        .ttl = LeaseDuration{2},
        .tick = LeaseTick{0}});

    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(store.transaction(transaction_request(
                {},
                {transaction_put("key", "value", false, 1)})));
        },
        "attachment without ownership comparison is invalid");
    expect(store.revision() == 0, "invalid attachment is atomic");

    const auto result = store.transaction(transaction_request(
        {},
        {transaction_put("key", "value", false, 1)},
        {transaction_range("a", "z")},
        {LeaseOwnership{LeaseId{1}, granted.lease.fencing_token}},
        LeaseTick{2}));
    expect(!result.succeeded, "expired ownership selects failure branch");
    expect(!store.get(bytes("key")).value, "expired lease cannot gain a key");
}

void lease_cleanup_revision_overflow_is_atomic() {
    const KeyValue attached{
        .key = bytes("key"),
        .value = bytes("value"),
        .version = 1,
        .create_revision = 1,
        .mod_revision = 1,
        .lease_id = 1};
    InMemoryMetadataStore store(InMemoryStoreSnapshot{
        .values = {attached},
        .leases = {kura::metadata::LeaseRecord{
            .id = LeaseId{1},
            .fencing_token = FencingToken{1},
            .granted_ttl = LeaseDuration{1},
            .expiry_tick = LeaseTick{1},
            .attached_keys = {bytes("key")}}},
        .revision = std::numeric_limits<std::int64_t>::max(),
        .logical_tick = LeaseTick{0},
        .next_lease_id = 2,
        .next_fencing_token = FencingToken{2}});

    expect_throws<std::overflow_error>(
        [&store] {
            static_cast<void>(store.expire_leases(LeaseTick{1}));
        },
        "lease cleanup must reject revision exhaustion");
    expect(
        store.get(bytes("key")).value == attached
            && store.time_to_live(
                         LeaseTimeToLiveRequest{
                             .id = LeaseId{1},
                             .tick = LeaseTick{1}})
                    .code
                == LeaseResultCode::expired,
        "failed cleanup must preserve lease and attached key");
}

void concurrent_kura_publishers_have_one_winner() {
    InMemoryMetadataStore store;
    const auto expected =
        store.put(bytes("/kura/current"), bytes("snapshot-1")).revision;
    std::barrier start(3);

    auto publish = [&store, expected, &start](
                       const std::string_view snapshot) {
        start.arrive_and_wait();
        return store.transaction(transaction_request(
            {Compare{
                .key = bytes("/kura/current"),
                .target = CompareTarget::mod_revision,
                .result = CompareResult::equal,
                .expected = expected}},
            {
                transaction_put(
                    std::string("/kura/snapshots/") + std::string(snapshot),
                    "manifest"),
                transaction_put("/kura/current", snapshot),
            },
            {transaction_range("/kura/current", "/kura/currenu")}));
    };

    auto first = std::async(std::launch::async, publish, "snapshot-2a");
    auto second = std::async(std::launch::async, publish, "snapshot-2b");
    start.arrive_and_wait();
    const auto first_result = first.get();
    const auto second_result = second.get();

    expect(
        static_cast<int>(first_result.succeeded)
                + static_cast<int>(second_result.succeeded)
            == 1,
        "exactly one concurrent publisher must win");
    expect(store.revision() == 2, "winning publication advances only once");
    const std::string winner =
        store.get(bytes("/kura/current")).value->value.to_string();
    const std::string loser =
        winner == "snapshot-2a" ? "snapshot-2b" : "snapshot-2a";
    expect(
        store.get(bytes("/kura/snapshots/" + winner)).value.has_value(),
        "winning snapshot and pointer publish atomically");
    expect(
        !store.get(bytes("/kura/snapshots/" + loser)).value.has_value(),
        "losing publisher must not leak its snapshot");
    expect(
        store.get(bytes("/kura/current")).value->mod_revision == 2
            && store.get(bytes("/kura/snapshots/" + winner))
                   .value->mod_revision
                == 2,
        "publication keys must share one revision");
}

}  // namespace

int main() {
    const std::vector<std::pair<std::string, void (*)()>> tests{
        {"byte sequence", byte_sequences_copy_input_and_compare_unsigned},
        {"binary prefix range", prefix_range_end_handles_binary_prefixes},
        {"unbounded prefix range", prefix_range_end_reports_unbounded_prefixes},
        {"prefix range immutability", prefix_range_end_preserves_input},
        {"put lifecycle", put_creates_and_updates_one_generation},
        {"erase lifecycle", erase_noop_does_not_advance_and_recreate_resets_version},
        {"compare and set", compare_and_set_uses_modification_revision},
        {"create compare and set", compare_and_set_zero_creates_only_when_absent},
        {"range", range_is_ordered_and_reports_one_revision},
        {"validation", invalid_keys_and_ranges_are_rejected},
        {"concurrent compare and set", concurrent_compare_and_set_has_one_winner},
        {"revision exhaustion", revision_exhaustion_does_not_partially_mutate},
        {"transaction comparisons", transactions_compare_every_target_and_result},
        {"transaction branches", transactions_select_success_and_failure_branches},
        {"transaction conjunction", transaction_comparisons_are_conjunctive_and_pretransactional},
        {"transaction revision", transaction_multi_key_writes_share_one_revision},
        {"transaction reads", transaction_reads_see_prior_writes_and_use_typed_results},
        {"transaction absent delete", absent_transaction_delete_is_a_noop},
        {"transaction duplicate writes", duplicate_transaction_writes_are_rejected},
        {"transaction atomic validation", invalid_selected_transaction_is_atomic},
        {"transaction overflow", transaction_overflow_is_atomic},
        {"lease grant keepalive TTL", lease_grant_keepalive_and_ttl_use_logical_ticks},
        {"lease revoke", lease_revoke_deletes_attached_keys_at_one_revision},
        {"lease paused owner fencing", paused_owner_is_fenced_after_expiry_and_regrant},
        {"lease expiry batch", expiry_batches_are_deterministic_after_a_pause},
        {"lease reattachment snapshot", reattachment_updates_indexes_and_round_trips_snapshot},
        {"leased put ownership", leased_put_requires_verified_live_ownership},
        {"lease cleanup overflow", lease_cleanup_revision_overflow_is_atomic},
        {"concurrent transaction publishers", concurrent_kura_publishers_have_one_winner}};

    int failures = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": " << error.what() << '\n';
        }
    }

    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }

    std::cout << tests.size() << " test(s) passed\n";
    return 0;
}
