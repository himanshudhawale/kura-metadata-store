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
#include "kura/metadata/kv/transaction_request.hpp"
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
#include <vector>

namespace {

using kura::metadata::ByteSequence;
using kura::metadata::CompareAndSetResult;
using kura::metadata::InMemoryMetadataStore;
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
        {"revision exhaustion", revision_exhaustion_does_not_partially_mutate}};

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
