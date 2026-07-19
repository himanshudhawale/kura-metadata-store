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
#include "kura/metadata/lease/lease_request.hpp"
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
#include <chrono>
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
using kura::metadata::KeyRange;
using kura::metadata::KeyValue;
using kura::metadata::LeaseGrantRequest;
using kura::metadata::LeaseId;
using kura::metadata::LeaseKeepAliveRequest;
using kura::metadata::LeaseRevokeRequest;
using kura::metadata::PutRequest;
using kura::metadata::PutResult;
using kura::metadata::RangeRead;
using kura::metadata::RangeRequest;
using kura::metadata::RequestOperation;
using kura::metadata::Revision;
using kura::metadata::StatusCode;
using kura::metadata::StoreError;
using kura::metadata::StoreLimits;
using kura::metadata::TransactionRequest;
using kura::metadata::WatchFilter;
using kura::metadata::WatchId;
using kura::metadata::WatchRequest;
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
    std::vector<RequestOperation> failure = {}) {
    return {
        .comparisons = std::move(comparisons),
        .success = std::move(success),
        .failure = std::move(failure)};
}

WatchRequest exact_watch(
    const std::int64_t id,
    const std::string_view key,
    const std::int64_t start_revision = 0,
    const WatchFilter filter = WatchFilter::include_all,
    const bool progress_notifications = false) {
    return {
        .id = WatchId{id},
        .range = KeyRange{.start = bytes(key)},
        .start_revision = Revision{start_revision},
        .filter = filter,
        .progress_notifications = progress_notifications};
}

WatchRequest range_watch(
    const std::int64_t id,
    const std::string_view start,
    const std::string_view end,
    const std::int64_t start_revision = 0) {
    return {
        .id = WatchId{id},
        .range = key_range(start, end),
        .start_revision = Revision{start_revision}};
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

void watches_select_exact_keys_and_ranges() {
    InMemoryMetadataStore store;
    static_cast<void>(store.create_watch(exact_watch(1, "a")));
    static_cast<void>(store.create_watch(range_watch(2, "a", "c")));

    static_cast<void>(store.put(bytes("a"), bytes("one")));
    static_cast<void>(store.put(bytes("b"), bytes("two")));
    static_cast<void>(store.put(bytes("c"), bytes("three")));

    const auto exact = store.poll_watch(WatchId{1});
    expect(exact && exact->events.size() == 1, "exact watch gets one event");
    expect(
        exact->events[0].mutation.current->key == bytes("a"),
        "exact watch selects its key");
    expect(!store.poll_watch(WatchId{1}), "exact watch excludes other keys");

    const auto first = store.poll_watch(WatchId{2});
    const auto second = store.poll_watch(WatchId{2});
    expect(
        first && second && first->header.revision == 1
            && second->header.revision == 2,
        "range watch preserves revision order");
    expect(
        first->events[0].mutation.current->key == bytes("a")
            && second->events[0].mutation.current->key == bytes("b"),
        "range watch uses a half-open interval");
    expect(!store.poll_watch(WatchId{2}), "range watch excludes its end");
}

void watches_batch_transactions_and_resume() {
    InMemoryMetadataStore store;
    static_cast<void>(store.create_watch(range_watch(1, "a", "z")));
    static_cast<void>(store.put(bytes("a"), bytes("one")));
    const auto first = store.poll_watch(WatchId{1});
    expect(first && first->header.revision == 1, "first revision delivered");
    static_cast<void>(store.cancel_watch(WatchId{1}));

    const auto transaction = store.transaction(transaction_request(
        {},
        {
            transaction_put("b", "two"),
            transaction_put("c", "three"),
        }));
    static_cast<void>(store.create_watch(
        range_watch(2, "a", "z", first->header.revision + 1)));
    const auto resumed = store.poll_watch(WatchId{2});
    expect(
        resumed && resumed->header.revision == transaction.header.revision,
        "resume starts at last seen plus one");
    expect(
        resumed->events.size() == 2,
        "one response contains the complete transaction batch");
    expect(
        resumed->events[0].mutation.current->key == bytes("b")
            && resumed->events[1].mutation.current->key == bytes("c"),
        "transaction events retain operation order");
    expect(!store.poll_watch(WatchId{2}), "transaction batch is unique");
}

void watches_progress_filter_and_cancel() {
    InMemoryMetadataStore store;
    static_cast<void>(store.create_watch(exact_watch(
        1,
        "key",
        0,
        WatchFilter::exclude_put)));
    static_cast<void>(store.put(bytes("key"), bytes("value")));
    expect(!store.poll_watch(WatchId{1}), "put filter suppresses puts");
    const auto erased = store.erase(bytes("key"));
    expect(erased.deleted, "test erase mutates");
    const auto event = store.poll_watch(WatchId{1});
    expect(
        event && event->events.size() == 1
            && !event->events[0].mutation.current
            && event->events[0].mutation.previous->value == bytes("value"),
        "erase event carries immutable previous state");

    store.request_watch_progress(WatchId{1});
    const auto progress = store.poll_watch(WatchId{1});
    expect(
        progress && progress->events.empty()
            && progress->header.revision == store.revision(),
        "explicit progress bookmarks current delivered history");

    static_cast<void>(store.create_watch(exact_watch(
        2,
        "missing",
        0,
        WatchFilter::include_all,
        true)));
    static_cast<void>(store.put(bytes("other"), bytes("value")));
    const auto automatic_progress = store.poll_watch(WatchId{2});
    expect(
        automatic_progress && automatic_progress->events.empty()
            && automatic_progress->header.revision == store.revision(),
        "enabled progress notifications bookmark nonmatching revisions");
    static_cast<void>(store.cancel_watch(WatchId{2}));

    static_cast<void>(store.create_watch(exact_watch(
        3,
        "filter",
        0,
        WatchFilter::exclude_erase)));
    static_cast<void>(store.put(bytes("filter"), bytes("value")));
    expect(
        store.poll_watch(WatchId{3})->events.size() == 1,
        "erase filter includes puts");
    static_cast<void>(store.erase(bytes("filter")));
    expect(!store.poll_watch(WatchId{3}), "erase filter suppresses erases");
    static_cast<void>(store.cancel_watch(WatchId{3}));

    const auto cancelled = store.cancel_watch(WatchId{1});
    expect(cancelled.cancelled, "cancellation returns a final response");
    expect_throws<StoreError>(
        [&store] {
            static_cast<void>(store.poll_watch(WatchId{1}));
        },
        "cancelled watch is released");
}

void watches_report_compacted_and_future_revisions() {
    StoreLimits limits;
    limits.max_watch_history_revisions = 2;
    InMemoryMetadataStore store(0, limits);
    static_cast<void>(store.put(bytes("a"), bytes("1")));
    static_cast<void>(store.put(bytes("b"), bytes("2")));
    static_cast<void>(store.put(bytes("c"), bytes("3")));
    expect(store.compact_revision() == 1, "history advances compaction boundary");

    try {
        static_cast<void>(
            store.create_watch(range_watch(1, "a", "z", 1)));
        throw TestFailure("compacted start must fail");
    } catch (const StoreError& error) {
        expect(
            error.code() == StatusCode::compacted
                && error.compact_revision() == 1,
            "compacted error reports its boundary");
    }
    try {
        static_cast<void>(
            store.create_watch(range_watch(2, "a", "z", 5)));
        throw TestFailure("future start must fail");
    } catch (const StoreError& error) {
        expect(
            error.code() == StatusCode::future_revision,
            "future revision has an explicit error");
    }
    static_cast<void>(
        store.create_watch(range_watch(3, "a", "z", 4)));
    expect(
        !store.poll_watch(WatchId{3}),
        "immediately following revision is a valid live cursor");
}

void watches_enforce_limits_and_backpressure() {
    StoreLimits limits;
    limits.max_watchers = 1;
    limits.max_watch_pending_responses = 1;
    limits.max_watch_history_revisions = 0;
    InMemoryMetadataStore store(limits);
    static_cast<void>(store.create_watch(range_watch(1, "a", "z")));
    try {
        static_cast<void>(store.create_watch(range_watch(2, "a", "z")));
        throw TestFailure("watcher limit must fail");
    } catch (const StoreError& error) {
        expect(
            error.code() == StatusCode::quota_exceeded,
            "watcher limit reports quota exhaustion");
    }

    static_cast<void>(store.put(bytes("a"), bytes("1")));
    static_cast<void>(store.put(bytes("b"), bytes("2")));
    const auto terminal = store.poll_watch(WatchId{1});
    expect(
        terminal && terminal->cancelled
            && terminal->status == StatusCode::quota_exceeded
            && terminal->events.empty(),
        "slow watcher is explicitly cancelled without a partial batch");
    expect(
        store.compact_revision() == 2,
        "zero history retains no completed revision");
}

void concurrent_watch_delivery_is_ordered_and_unique() {
    InMemoryMetadataStore store;
    static_cast<void>(store.create_watch(range_watch(1, "k", "l")));
    constexpr int writer_count = 32;
    std::barrier start(writer_count + 1);
    std::vector<std::future<PutResult>> writers;
    writers.reserve(writer_count);
    for (int index = 0; index < writer_count; ++index) {
        writers.push_back(std::async(
            std::launch::async,
            [&store, &start, index] {
                start.arrive_and_wait();
                return store.put(
                    bytes("k" + std::to_string(index)),
                    bytes("value"));
            }));
    }
    start.arrive_and_wait();
    for (auto& writer : writers) {
        static_cast<void>(writer.get());
    }

    for (std::int64_t revision = 1;
         revision <= writer_count;
         ++revision) {
        const auto response = store.poll_watch(WatchId{1});
        expect(response.has_value(), "every concurrent mutation is delivered");
        expect(
            response->header.revision == revision
                && response->events.size() == 1
                && response->events[0].revision.value == revision,
            "concurrent delivery has no duplicate or reordered revision");
    }
    expect(!store.poll_watch(WatchId{1}), "concurrent delivery has no extras");
}

void failed_mutations_emit_no_watch_events() {
    InMemoryMetadataStore store;
    static_cast<void>(store.put(bytes("key"), bytes("value")));
    static_cast<void>(store.create_watch(range_watch(1, "a", "z")));

    const auto failed_cas =
        store.compare_and_set(bytes("key"), 0, bytes("wrong"));
    expect(!failed_cas.succeeded, "test CAS must fail");
    expect(!store.erase(bytes("absent")).deleted, "test delete must be absent");
    const auto failed_branch = store.transaction(transaction_request(
        {Compare{
            .key = bytes("key"),
            .target = CompareTarget::value,
            .result = CompareResult::equal,
            .expected = bytes("wrong")}},
        {transaction_put("leak", "value")}));
    expect(!failed_branch.succeeded, "test comparison must fail");
    const auto absent_transaction_delete = store.transaction(
        transaction_request({}, {transaction_delete("x", "y")}));
    expect(
        std::get<DeleteRangeResult>(
            absent_transaction_delete.responses[0]).deleted == 0,
        "test transaction delete must be absent");
    expect_throws<std::invalid_argument>(
        [&store] {
            static_cast<void>(store.transaction(transaction_request(
                {},
                {
                    transaction_put("duplicate", "one"),
                    transaction_put("duplicate", "two"),
                })));
        },
        "test transaction must be rejected");
    expect(
        !store.poll_watch(WatchId{1}),
        "failed and no-op mutations emit no watch event");
}

void leases_grant_keep_alive_and_report_ttl() {
    using namespace std::chrono_literals;
    InMemoryMetadataStore store;
    const auto start = kura::metadata::Clock::TimePoint{};
    const auto granted = store.grant_lease(
        LeaseGrantRequest{.ttl = 10s},
        start);

    expect(granted.lease.id.value == 1, "automatic lease IDs start positive");
    expect(
        granted.lease.granted_ttl == 10s
            && granted.lease.remaining_ttl == 10s,
        "grant reports its full TTL");
    expect(
        store.time_to_live(granted.lease.id, start + 3s)
                .lease.remaining_ttl
            == 7s,
        "TTL lookup reports remaining logical time");

    const auto renewed = store.keep_alive(
        LeaseKeepAliveRequest{.id = granted.lease.id},
        start + 5s);
    expect(
        renewed.lease.remaining_ttl == 10s
            && store.time_to_live(granted.lease.id, start + 12s)
                    .lease.remaining_ttl
                == 3s,
        "keepalive resets the deadline from its apply time");
    expect(store.revision() == 0, "lease timing alone does not change KV revision");
}

void lease_transactions_attach_reattach_and_fence() {
    using namespace std::chrono_literals;
    InMemoryMetadataStore store;
    const auto now = kura::metadata::Clock::TimePoint{};
    const LeaseId first = store.grant_lease(
        LeaseGrantRequest{.requested_id = LeaseId{10}, .ttl = 30s},
        now).lease.id;
    const LeaseId second = store.grant_lease(
        LeaseGrantRequest{.requested_id = LeaseId{20}, .ttl = 30s},
        now).lease.id;

    const auto attached = store.transaction(transaction_request(
        {},
        {transaction_put("owner", "writer-a", false, first.value)}));
    expect(
        std::get<PutResult>(attached.responses[0]).current.lease_id
            == first.value,
        "transaction put attaches a live lease");

    const auto fenced = store.transaction(transaction_request(
        {Compare{
            .key = bytes("owner"),
            .target = CompareTarget::lease_id,
            .result = CompareResult::equal,
            .expected = first.value}},
        {transaction_put("published", "snapshot")}));
    expect(fenced.succeeded, "lease comparison fences protected writes");

    const auto reattached = store.transaction(transaction_request(
        {},
        {transaction_put("owner", "writer-b", false, second.value)}));
    expect(
        std::get<PutResult>(reattached.responses[0]).current.lease_id
            == second.value,
        "updating a key can move it to another lease");
    static_cast<void>(store.revoke_lease(
        LeaseRevokeRequest{.id = first}));
    expect(
        store.get(bytes("owner")).value.has_value(),
        "revoking the old lease does not delete a reattached key");
}

void lease_revoke_and_expiry_cascade_atomically() {
    using namespace std::chrono_literals;
    InMemoryMetadataStore store;
    const auto now = kura::metadata::Clock::TimePoint{};
    const LeaseId first = store.grant_lease(
        LeaseGrantRequest{.ttl = 5s},
        now).lease.id;
    const LeaseId second = store.grant_lease(
        LeaseGrantRequest{.ttl = 5s},
        now).lease.id;
    static_cast<void>(store.transaction(transaction_request(
        {},
        {
            transaction_put("a", "1", false, first.value),
            transaction_put("b", "2", false, first.value),
            transaction_put("c", "3", false, second.value),
        })));
    static_cast<void>(store.create_watch(range_watch(1, "a", "z")));

    const auto revoked = store.revoke_lease(
        LeaseRevokeRequest{.id = first});
    expect(
        revoked.header.revision == 2
            && !store.get(bytes("a")).value
            && !store.get(bytes("b")).value
            && store.get(bytes("c")).value.has_value(),
        "revoke removes all and only attached keys in one revision");
    const auto revoke_events = store.poll_watch(WatchId{1});
    expect(
        revoke_events && revoke_events->events.size() == 2
            && revoke_events->events[0].revision.value == 2
            && revoke_events->events[1].revision.value == 2,
        "revoke publishes one atomic ordered watch batch");

    expect(store.expire_leases(now + 4s) == 0, "lease is live before deadline");
    expect(store.expire_leases(now + 5s) == 1, "deadline expires the lease");
    expect(
        !store.get(bytes("c")).value && store.revision() == 3,
        "expiry cascade uses one mutation revision");
    expect_throws<StoreError>(
        [&store, second, now] {
            static_cast<void>(store.keep_alive(
                LeaseKeepAliveRequest{.id = second},
                now));
        },
        "expired leases cannot be renewed after expiry is applied");
}

void lease_validation_and_limits_are_explicit() {
    using namespace std::chrono_literals;
    StoreLimits limits;
    limits.max_active_leases = 1;
    InMemoryMetadataStore store(limits);
    const auto now = kura::metadata::Clock::TimePoint{};
    static_cast<void>(store.grant_lease(
        LeaseGrantRequest{.requested_id = LeaseId{7}, .ttl = 1s},
        now));

    expect_throws<StoreError>(
        [&store, now] {
            static_cast<void>(store.grant_lease(
                LeaseGrantRequest{.ttl = 1s},
                now));
        },
        "active lease limit must reject grants");
    expect_throws<StoreError>(
        [&store] {
            static_cast<void>(store.transaction(transaction_request(
                {},
                {transaction_put("key", "value", false, 99)})));
        },
        "transaction cannot attach an unknown lease");
    expect(!store.get(bytes("key")).value, "failed attachment is atomic");
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
        {"concurrent transaction publishers", concurrent_kura_publishers_have_one_winner},
        {"watch key and range", watches_select_exact_keys_and_ranges},
        {"watch transaction resume", watches_batch_transactions_and_resume},
        {"watch progress filter cancel", watches_progress_filter_and_cancel},
        {"watch revision errors", watches_report_compacted_and_future_revisions},
        {"watch limits", watches_enforce_limits_and_backpressure},
        {"watch concurrent delivery", concurrent_watch_delivery_is_ordered_and_unique},
        {"watch failed mutations", failed_mutations_emit_no_watch_events},
        {"lease TTL lifecycle", leases_grant_keep_alive_and_report_ttl},
        {"lease transaction fencing", lease_transactions_attach_reattach_and_fence},
        {"lease cascade deletion", lease_revoke_and_expiry_cascade_atomically},
        {"lease validation", lease_validation_and_limits_are_explicit}};

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
