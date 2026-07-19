#include "kura/metadata/storage/checksum.hpp"
#include "kura/metadata/storage/snapshot_store.hpp"
#include "kura/metadata/storage/storage_error.hpp"
#include "kura/metadata/storage/wal.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using kura::metadata::Durability;
using kura::metadata::FileSnapshotStore;
using kura::metadata::SegmentedWriteAheadLog;
using kura::metadata::Snapshot;
using kura::metadata::StorageError;
using kura::metadata::StorageLimits;
using kura::metadata::WalEntry;
using kura::metadata::WalRecordType;

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
            ("storage-test-" +
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

StorageLimits small_limits() {
    StorageLimits limits;
    limits.wal_segment_bytes = 124;
    limits.max_wal_payload_bytes = 20;
    limits.max_recovery_records = 100;
    limits.max_snapshot_bytes = 1U << 20U;
    limits.max_snapshot_members = 16;
    return limits;
}

WalEntry entry(
    const std::uint64_t index,
    std::vector<std::uint8_t> payload = {1, 2}) {
    return {
        .type = WalRecordType::command,
        .term = {7},
        .index = {index},
        .payload = std::move(payload)};
}

Snapshot snapshot(const std::uint64_t index) {
    Snapshot value;
    value.metadata.last_included_index.value = index;
    value.metadata.last_included_term.value = 3;
    value.metadata.store_revision.value = static_cast<std::int64_t>(index + 10);
    value.metadata.compaction_revision.value =
        static_cast<std::int64_t>(index);
    value.metadata.membership.voters = {{1}, {2}};
    value.metadata.membership.learners = {{3}};
    value.metadata.format_version = 1;
    value.state = {9, 8, 7, static_cast<std::uint8_t>(index)};
    return value;
}

std::vector<std::filesystem::path> files_with_extension(
    const std::filesystem::path& directory,
    const std::string& extension) {
    std::vector<std::filesystem::path> paths;
    if (!std::filesystem::exists(directory)) {
        return paths;
    }
    for (const auto& item : std::filesystem::directory_iterator(directory)) {
        if (item.is_regular_file() &&
            item.path().extension().string() == extension) {
            paths.push_back(item.path());
        }
    }
    std::sort(paths.begin(), paths.end());
    return paths;
}

std::vector<std::uint8_t> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    expect(static_cast<bool>(input), "test could not open file");
    const auto length = input.tellg();
    expect(length >= 0, "test could not measure file");
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
    input.seekg(0);
    input.read(
        reinterpret_cast<char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    expect(static_cast<bool>(input), "test could not read file");
    return bytes;
}

void write_bytes(
    const std::filesystem::path& path,
    const std::span<const std::uint8_t> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    expect(static_cast<bool>(output), "test could not open file for writing");
    output.write(
        reinterpret_cast<const char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    expect(static_cast<bool>(output), "test could not write file");
}

void set_u32(
    std::vector<std::uint8_t>& bytes,
    const std::size_t offset,
    const std::uint32_t value) {
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        bytes[offset + i] =
            static_cast<std::uint8_t>((value >> (8U * i)) & 0xffU);
    }
}

void set_u64(
    std::vector<std::uint8_t>& bytes,
    const std::size_t offset,
    const std::uint64_t value) {
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        bytes[offset + i] =
            static_cast<std::uint8_t>((value >> (8U * i)) & 0xffU);
    }
}

void repair_record_checksum(
    std::vector<std::uint8_t>& bytes,
    const std::size_t record_offset,
    const std::size_t payload_size) {
    std::vector<std::uint8_t> scope;
    scope.insert(
        scope.end(),
        bytes.begin() + static_cast<std::ptrdiff_t>(record_offset),
        bytes.begin() + static_cast<std::ptrdiff_t>(record_offset + 36));
    scope.insert(
        scope.end(),
        bytes.begin() +
            static_cast<std::ptrdiff_t>(record_offset + 40),
        bytes.begin() +
            static_cast<std::ptrdiff_t>(record_offset + 40 + payload_size));
    set_u32(bytes, record_offset + 36, kura::metadata::crc32c(scope));
}

void append_all(
    SegmentedWriteAheadLog& wal,
    const std::vector<WalEntry>& entries) {
    wal.append(entries, Durability::synchronize);
}

void test_crc_and_record_round_trip() {
    const std::array<std::uint8_t, 9> vector{
        '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    expect(
        kura::metadata::crc32c({}) == 0,
        "empty CRC32C vector differs");
    expect(
        kura::metadata::crc32c(vector) == 0xe3069283U,
        "CRC32C check vector differs");

    TestDirectory directory;
    const auto wal_path = directory.path() / "wal";
    const auto original = entry(11, {0, 1, 2, 255});
    {
        SegmentedWriteAheadLog wal(wal_path, small_limits());
        wal.append(std::span(&original, 1), Durability::synchronize);
        expect(wal.recover() == std::vector<WalEntry>{original}, "record round-trip failed");
    }
    SegmentedWriteAheadLog reopened(wal_path, small_limits());
    expect(
        reopened.recover() == std::vector<WalEntry>{original},
        "record did not survive reopen");
}

void test_empty_multi_record_and_rotation() {
    TestDirectory directory;
    const auto wal_path = directory.path() / "wal";
    SegmentedWriteAheadLog wal(wal_path, small_limits());
    expect(wal.recover().empty(), "empty WAL did not recover empty");
    const std::vector<WalEntry> entries{
        entry(1), entry(2), entry(3), entry(4), entry(5)};
    append_all(wal, entries);
    expect(wal.recover() == entries, "multi-record recovery differs");
    expect(
        files_with_extension(wal_path, ".kwal").size() == 3,
        "segment rotation count differs");
}

void test_torn_final_header_and_payload() {
    {
        TestDirectory directory;
        const auto wal_path = directory.path() / "wal";
        {
            SegmentedWriteAheadLog wal(wal_path, small_limits());
            const auto value = entry(1);
            wal.append(std::span(&value, 1), Durability::synchronize);
        }
        auto segment = files_with_extension(wal_path, ".kwal").front();
        auto bytes = read_bytes(segment);
        bytes.insert(bytes.end(), 13, 0xaa);
        write_bytes(segment, bytes);
        SegmentedWriteAheadLog recovered(wal_path, small_limits());
        expect(recovered.recover().size() == 1, "torn final header lost valid prefix");
        expect(
            std::filesystem::file_size(segment) == 82,
            "torn final header was not removed");
    }
    {
        TestDirectory directory;
        const auto wal_path = directory.path() / "wal";
        {
            SegmentedWriteAheadLog wal(wal_path, small_limits());
            append_all(wal, {entry(1), entry(2)});
        }
        auto segment = files_with_extension(wal_path, ".kwal").front();
        std::filesystem::resize_file(
            segment,
            std::filesystem::file_size(segment) - 1);
        SegmentedWriteAheadLog recovered(wal_path, small_limits());
        const auto entries = recovered.recover();
        expect(
            entries == std::vector<WalEntry>{entry(1)},
            "torn final payload did not restore one valid prefix");
    }
}

void test_every_wal_truncation_boundary() {
    for (std::uint64_t cut = 0; cut < 124; ++cut) {
        TestDirectory directory;
        const auto wal_path = directory.path() / "wal";
        {
            SegmentedWriteAheadLog wal(wal_path, small_limits());
            append_all(wal, {entry(1), entry(2)});
        }
        const auto segment = files_with_extension(wal_path, ".kwal").front();
        std::filesystem::resize_file(segment, cut);
        if (cut < 40) {
            expect_throws<StorageError>(
                [&] { SegmentedWriteAheadLog invalid(wal_path, small_limits()); },
                "truncated segment header was accepted");
            continue;
        }
        SegmentedWriteAheadLog recovered(wal_path, small_limits());
        const auto entries = recovered.recover();
        const auto expected_count =
            cut < 82 ? std::size_t{0} : std::size_t{1};
        expect(
            entries.size() == expected_count,
            "WAL truncation boundary restored the wrong prefix");
    }
}

void test_malformed_and_corrupt_records() {
    const auto expect_corruption = [](
        const std::function<void(
            const std::filesystem::path&,
            std::vector<std::uint8_t>&)>& mutate,
        const std::string& message) {
        TestDirectory directory;
        const auto wal_path = directory.path() / "wal";
        {
            SegmentedWriteAheadLog wal(wal_path, small_limits());
            append_all(wal, {entry(1), entry(2), entry(3)});
        }
        auto segments = files_with_extension(wal_path, ".kwal");
        auto bytes = read_bytes(segments.front());
        mutate(segments.front(), bytes);
        if (!bytes.empty()) {
            write_bytes(segments.front(), bytes);
        }
        expect_throws<StorageError>(
            [&] { SegmentedWriteAheadLog invalid(wal_path, small_limits()); },
            message);
    };

    expect_corruption(
        [](const auto& path, auto& bytes) {
            static_cast<void>(bytes);
            std::filesystem::resize_file(path, 100);
            bytes.clear();
        },
        "interior truncated header was accepted");
    expect_corruption(
        [](const auto&, auto& bytes) {
            set_u64(bytes, 40 + 28, 21);
        },
        "invalid payload length was accepted");
    expect_corruption(
        [](const auto&, auto& bytes) {
            bytes[40 + 36] ^= 0x80;
        },
        "bad record checksum was accepted");
    expect_corruption(
        [](const auto&, auto& bytes) {
            bytes[40 + 40] ^= 0x40;
        },
        "interior payload corruption was accepted");
}

void test_index_gap_and_regression() {
    const auto run = [](const std::uint64_t replacement) {
        TestDirectory directory;
        const auto wal_path = directory.path() / "wal";
        auto limits = small_limits();
        limits.wal_segment_bytes = 200;
        {
            SegmentedWriteAheadLog wal(wal_path, limits);
            append_all(wal, {entry(5), entry(6)});
        }
        auto segment = files_with_extension(wal_path, ".kwal").front();
        auto bytes = read_bytes(segment);
        const std::size_t second_record = 82;
        set_u64(bytes, second_record + 20, replacement);
        repair_record_checksum(bytes, second_record, 2);
        write_bytes(segment, bytes);
        expect_throws<StorageError>(
            [&] { SegmentedWriteAheadLog invalid(wal_path, limits); },
            "index ordering corruption was accepted");
    };
    run(7);
    run(5);
}

void test_snapshot_round_trip_and_integrity() {
    TestDirectory directory;
    const auto snapshot_path = directory.path() / "snapshots";
    FileSnapshotStore store(snapshot_path, small_limits());
    expect(!store.latest(), "empty snapshot store was not empty");
    const auto first = snapshot(5);
    store.publish(first);
    const auto restored = store.latest();
    expect(restored.has_value(), "published snapshot was not discovered");
    expect(restored->metadata.last_included_index.value == 5, "snapshot index differs");
    expect(restored->metadata.membership.voters.size() == 2, "snapshot voters differ");
    expect(restored->state == first.state, "snapshot state differs");
    expect(restored->checksum != 0, "snapshot checksum was not exposed");

    store.publish(snapshot(6));
    auto files = files_with_extension(snapshot_path, ".ksnap");
    auto bytes = read_bytes(files.back());
    bytes.back() ^= 1;
    write_bytes(files.back(), bytes);
    expect_throws<StorageError>(
        [&] { static_cast<void>(store.validate(files.back())); },
        "snapshot checksum corruption was accepted");
    const auto fallback = store.latest();
    expect(
        fallback && fallback->metadata.last_included_index.value == 5,
        "latest-valid discovery did not fall back");
}

void test_partial_and_failed_snapshot_publication() {
    TestDirectory directory;
    const auto snapshot_path = directory.path() / "snapshots";
    FileSnapshotStore store(snapshot_path, small_limits());
    store.publish(snapshot(9));
    {
        std::ofstream partial(
            snapshot_path / "snapshot-00000000000000000010.ksnap.tmp.partial",
            std::ios::binary);
        partial << "partial";
    }
    expect(
        store.latest()->metadata.last_included_index.value == 9,
        "partial temporary snapshot replaced current snapshot");
    expect_throws<StorageError>(
        [&] { store.publish(snapshot(9)); },
        "non-advancing snapshot publication was accepted");
    expect(
        store.latest()->metadata.last_included_index.value == 9,
        "failed publication replaced current snapshot");
}

void test_wal_truncation_coverage() {
    TestDirectory directory;
    const auto wal_path = directory.path() / "wal";
    const auto snapshot_path = directory.path() / "snapshots";
    SegmentedWriteAheadLog wal(wal_path, small_limits());
    append_all(wal, {entry(1), entry(2), entry(3), entry(4)});
    FileSnapshotStore snapshots(snapshot_path, small_limits());
    expect_throws<StorageError>(
        [&] { wal.truncate_through({2}, snapshots); },
        "WAL truncation without snapshot was accepted");
    snapshots.publish(snapshot(2));
    expect_throws<StorageError>(
        [&] { wal.truncate_through({3}, snapshots); },
        "WAL truncation beyond snapshot was accepted");
    wal.truncate_through({2}, snapshots);
    expect(
        wal.recover() == std::vector<WalEntry>({entry(3), entry(4)}),
        "covered WAL segment was not conservatively truncated");
    expect(
        files_with_extension(wal_path, ".kwal").size() == 1,
        "truncation removed an unexpected number of segments");
}

void test_overflow_and_limits() {
    {
        TestDirectory directory;
        SegmentedWriteAheadLog wal(directory.path() / "wal", small_limits());
        const auto maximum = entry(std::numeric_limits<std::uint64_t>::max());
        wal.append(std::span(&maximum, 1), Durability::synchronize);
        expect_throws<StorageError>(
            [&] {
                const auto another =
                    entry(std::numeric_limits<std::uint64_t>::max());
                wal.append(std::span(&another, 1), Durability::synchronize);
            },
            "index overflow was accepted");
    }
    {
        TestDirectory directory;
        auto limits = small_limits();
        limits.max_wal_payload_bytes = 3;
        SegmentedWriteAheadLog wal(directory.path() / "wal", limits);
        const auto oversized = entry(1, {1, 2, 3, 4});
        expect_throws<StorageError>(
            [&] { wal.append(std::span(&oversized, 1), Durability::synchronize); },
            "payload limit was not enforced");
    }
    {
        TestDirectory directory;
        auto limits = small_limits();
        limits.max_recovery_records = 1;
        SegmentedWriteAheadLog wal(directory.path() / "wal", limits);
        const auto first = entry(1);
        wal.append(std::span(&first, 1), Durability::synchronize);
        const auto second = entry(2);
        expect_throws<StorageError>(
            [&] { wal.append(std::span(&second, 1), Durability::synchronize); },
            "record count limit was not enforced");
    }
    {
        TestDirectory directory;
        auto limits = small_limits();
        limits.max_snapshot_bytes = 72;
        FileSnapshotStore store(directory.path() / "snapshots", limits);
        expect_throws<StorageError>(
            [&] { store.publish(snapshot(1)); },
            "snapshot size limit was not enforced");
        auto invalid_revision = snapshot(1);
        invalid_revision.metadata.store_revision.value = -1;
        expect_throws<StorageError>(
            [&] { store.publish(invalid_revision); },
            "negative/overflowed revision was accepted");
    }
}

}  // namespace

int main() {
    const std::array tests{
        test_crc_and_record_round_trip,
        test_empty_multi_record_and_rotation,
        test_torn_final_header_and_payload,
        test_every_wal_truncation_boundary,
        test_malformed_and_corrupt_records,
        test_index_gap_and_regression,
        test_snapshot_round_trip_and_integrity,
        test_partial_and_failed_snapshot_publication,
        test_wal_truncation_coverage,
        test_overflow_and_limits};
    try {
        for (const auto test : tests) {
            test();
        }
    } catch (const std::exception& error) {
        std::cerr << "durable storage test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "durable storage tests passed\n";
    return 0;
}
