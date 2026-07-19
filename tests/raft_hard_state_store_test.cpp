#include "kura/metadata/storage/raft_hard_state_store.hpp"

#include "kura/metadata/storage/storage_error.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using kura::metadata::FileRaftHardStateStore;
using kura::metadata::NodeId;
using kura::metadata::PersistRaftHardState;
using kura::metadata::RaftHardState;
using kura::metadata::RaftHardStateWriteBoundary;
using kura::metadata::StorageError;
using kura::metadata::StorageLimits;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class SimulatedCrash final : public std::runtime_error {
public:
    SimulatedCrash() : std::runtime_error("simulated crash") {}
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename Exception, typename Operation>
void expect_throws(Operation&& operation, const std::string& message) {
    try {
        std::forward<Operation>(operation)();
    } catch (const Exception&) {
        return;
    }
    throw TestFailure(message);
}

class TestDirectory {
public:
    TestDirectory() {
        static std::atomic<std::uint64_t> counter{0};
        path_ = std::filesystem::current_path() /
            ("hard-state-test-" +
             std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const {
        return path_;
    }

private:
    std::filesystem::path path_;
};

RaftHardState state(
    const std::uint64_t term,
    const std::uint64_t voted_for = 0) {
    return {
        .current_term = {term},
        .voted_for = voted_for == 0
            ? std::nullopt
            : std::optional<NodeId>(NodeId{voted_for})};
}

PersistRaftHardState request(
    const std::uint64_t request_id,
    const RaftHardState& value) {
    return {
        .request_id = request_id,
        .state = value};
}

std::filesystem::path state_path(const TestDirectory& directory) {
    return directory.path() / "raft-hard-state.krhs";
}

std::vector<std::uint8_t> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    expect(static_cast<bool>(input), "test could not open hard-state file");
    const auto length = input.tellg();
    expect(length >= 0, "test could not measure hard-state file");
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
    input.seekg(0);
    input.read(
        reinterpret_cast<char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    expect(static_cast<bool>(input), "test could not read hard-state file");
    return bytes;
}

void write_bytes(
    const std::filesystem::path& path,
    const std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    expect(
        static_cast<bool>(output),
        "test could not open hard-state file for writing");
    output.write(
        reinterpret_cast<const char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    expect(static_cast<bool>(output), "test could not write hard-state file");
}

void test_round_trip_and_explicit_completion_event() {
    TestDirectory directory;
    std::vector<RaftHardStateWriteBoundary> boundaries;
    FileRaftHardStateStore store(
        directory.path(),
        {},
        [&](const auto boundary) { boundaries.push_back(boundary); });
    expect(store.load() == state(0), "fresh hard state was not term zero");

    const auto completion = store.persist(request(17, state(4, 9)));
    expect(completion.request_id == 17, "completion request ID differs");
    expect(completion.state == state(4, 9), "completion state differs");
    const std::array expected{
        RaftHardStateWriteBoundary::temporary_created,
        RaftHardStateWriteBoundary::record_written,
        RaftHardStateWriteBoundary::temporary_synchronized,
        RaftHardStateWriteBoundary::temporary_closed,
        RaftHardStateWriteBoundary::final_replaced,
        RaftHardStateWriteBoundary::directory_synchronized};
    expect(
        std::ranges::equal(boundaries, expected),
        "completion was produced without all durability boundaries");

    FileRaftHardStateStore reopened(directory.path());
    expect(reopened.load() == state(4, 9), "hard state did not survive reopen");
}

void test_transition_rules_prevent_two_votes() {
    TestDirectory directory;
    FileRaftHardStateStore store(directory.path());
    static_cast<void>(store.persist(request(1, state(8))));
    static_cast<void>(store.persist(request(2, state(8, 11))));
    expect_throws<StorageError>(
        [&] {
            static_cast<void>(store.persist(request(3, state(8, 12))));
        },
        "a second candidate was accepted in one term");
    expect_throws<StorageError>(
        [&] {
            static_cast<void>(store.persist(request(4, state(8))));
        },
        "a vote was cleared within its term");
    expect_throws<StorageError>(
        [&] {
            static_cast<void>(store.persist(request(5, state(7, 11))));
        },
        "currentTerm regression was accepted");

    static_cast<void>(store.persist(request(6, state(9))));
    static_cast<void>(store.persist(request(7, state(9, 12))));
    FileRaftHardStateStore reopened(directory.path());
    expect(reopened.load() == state(9, 12), "higher-term vote was not durable");
    expect_throws<StorageError>(
        [&] {
            static_cast<void>(reopened.persist(request(8, state(9, 11))));
        },
        "restart permitted a second vote in one term");
}

void test_crash_at_every_write_boundary() {
    const std::array boundaries{
        RaftHardStateWriteBoundary::temporary_created,
        RaftHardStateWriteBoundary::record_written,
        RaftHardStateWriteBoundary::temporary_synchronized,
        RaftHardStateWriteBoundary::temporary_closed,
        RaftHardStateWriteBoundary::final_replaced,
        RaftHardStateWriteBoundary::directory_synchronized};

    for (const auto crash_at : boundaries) {
        TestDirectory directory;
        {
            FileRaftHardStateStore initial(directory.path());
            static_cast<void>(initial.persist(request(1, state(21))));
        }

        bool completion_emitted = false;
        {
            FileRaftHardStateStore crashing(
                directory.path(),
                {},
                [=](const auto boundary) {
                    if (boundary == crash_at) {
                        throw SimulatedCrash();
                    }
                });
            expect_throws<SimulatedCrash>(
                [&] {
                    const auto completion =
                        crashing.persist(request(2, state(21, 101)));
                    static_cast<void>(completion);
                    completion_emitted = true;
                },
                "fault injection did not interrupt persistence");
            expect(!completion_emitted, "crashed write emitted a completion");
            expect_throws<StorageError>(
                [&] { static_cast<void>(crashing.load()); },
                "failed writer remained usable with ambiguous state");
        }

        FileRaftHardStateStore restarted(directory.path());
        const auto recovered = restarted.load();
        expect(
            recovered == state(21) || recovered == state(21, 101),
            "crash recovered neither the old nor new complete state");
        if (recovered == state(21, 101)) {
            expect_throws<StorageError>(
                [&] {
                    static_cast<void>(
                        restarted.persist(request(3, state(21, 202))));
                },
                "restart granted a second vote after ambiguous completion");
        } else {
            const auto completion =
                restarted.persist(request(3, state(21, 202)));
            expect(
                completion.state == state(21, 202),
                "unacknowledged vote prevented a safe replacement vote");
        }
    }
}

void test_every_torn_and_corrupt_record_boundary() {
    TestDirectory original_directory;
    {
        FileRaftHardStateStore store(original_directory.path());
        static_cast<void>(store.persist(request(1, state(33, 44))));
    }
    const auto valid = read_bytes(state_path(original_directory));
    expect(valid.size() == 48, "v1 hard-state record size differs");

    for (std::size_t cut = 0; cut < valid.size(); ++cut) {
        TestDirectory directory;
        write_bytes(state_path(directory), std::span(valid).first(cut));
        expect_throws<StorageError>(
            [&] { FileRaftHardStateStore invalid(directory.path()); },
            "torn hard-state record was accepted");
    }
    for (std::size_t offset = 0; offset < valid.size(); ++offset) {
        TestDirectory directory;
        auto corrupt = valid;
        corrupt[offset] ^= 0x80U;
        write_bytes(state_path(directory), corrupt);
        expect_throws<StorageError>(
            [&] { FileRaftHardStateStore invalid(directory.path()); },
            "corrupt hard-state byte was accepted");
    }

    TestDirectory trailing_directory;
    auto trailing = valid;
    trailing.push_back(0);
    write_bytes(state_path(trailing_directory), trailing);
    expect_throws<StorageError>(
        [&] { FileRaftHardStateStore invalid(trailing_directory.path()); },
        "hard-state trailing bytes were accepted");
}

void test_limits_stale_temporary_and_no_fallback() {
    {
        TestDirectory directory;
        auto limits = StorageLimits{};
        limits.max_raft_hard_state_bytes = 47;
        expect_throws<StorageError>(
            [&] { FileRaftHardStateStore invalid(directory.path(), limits); },
            "impossible hard-state size limit was accepted");
    }
    {
        TestDirectory directory;
        FileRaftHardStateStore store(directory.path());
        expect_throws<StorageError>(
            [&] {
                static_cast<void>(store.persist(request(0, state(1))));
            },
            "zero persistence request ID was accepted");
        expect_throws<StorageError>(
            [&] {
                auto invalid = state(1);
                invalid.voted_for = NodeId{0};
                static_cast<void>(store.persist(request(1, invalid)));
            },
            "zero node ID was accepted");
        expect_throws<StorageError>(
            [&] {
                static_cast<void>(store.persist(request(1, state(0, 1))));
            },
            "a vote in bootstrap term zero was accepted");
    }
    {
        TestDirectory directory;
        FileRaftHardStateStore store(directory.path());
        static_cast<void>(store.persist(request(1, state(5, 6))));
        const auto valid = read_bytes(state_path(directory));
        write_bytes(
            directory.path() / "raft-hard-state.krhs.tmp.stale",
            valid);
        auto corrupt = valid;
        corrupt[24] ^= 1;
        write_bytes(state_path(directory), corrupt);
        expect_throws<StorageError>(
            [&] { FileRaftHardStateStore invalid(directory.path()); },
            "corrupt final record fell back to a temporary file");
    }
}

}  // namespace

int main() {
    const std::array tests{
        test_round_trip_and_explicit_completion_event,
        test_transition_rules_prevent_two_votes,
        test_crash_at_every_write_boundary,
        test_every_torn_and_corrupt_record_boundary,
        test_limits_stale_temporary_and_no_fallback};
    try {
        for (const auto test : tests) {
            test();
        }
    } catch (const std::exception& error) {
        std::cerr << "Raft hard-state test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "Raft hard-state tests passed\n";
    return 0;
}
