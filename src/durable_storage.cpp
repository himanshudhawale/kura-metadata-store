#include "kura/metadata/storage/checksum.hpp"
#include "kura/metadata/storage/snapshot_store.hpp"
#include "kura/metadata/storage/storage_error.hpp"
#include "kura/metadata/storage/wal.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace kura::metadata {
namespace {

constexpr std::size_t wal_segment_header_size = 40;
constexpr std::size_t wal_record_header_size = 40;
constexpr std::size_t snapshot_header_size = 68;
constexpr std::uint16_t storage_format_version = 1;
constexpr std::uint16_t command_record_type = 1;
constexpr std::array<std::uint8_t, 8> wal_magic{
    'K', 'U', 'R', 'A', 'W', 'A', 'L', '1'};
constexpr std::array<std::uint8_t, 4> record_magic{'K', 'R', 'E', 'C'};
constexpr std::array<std::uint8_t, 8> snapshot_magic{
    'K', 'U', 'R', 'A', 'S', 'N', 'P', '1'};

[[noreturn]] void fail(const std::string& message) {
    throw StorageError(message);
}

std::string path_text(const std::filesystem::path& path) {
    return path.string();
}

std::string system_message(const std::string& action, const int error) {
    return action + ": " + std::system_category().message(error);
}

template <typename Integer>
void append_le(std::vector<std::uint8_t>& bytes, Integer value) {
    using Unsigned = std::make_unsigned_t<Integer>;
    auto current = static_cast<Unsigned>(value);
    for (std::size_t i = 0; i < sizeof(Integer); ++i) {
        bytes.push_back(static_cast<std::uint8_t>(current & 0xffU));
        current >>= 8U;
    }
}

template <typename Integer>
Integer read_le(
    const std::span<const std::uint8_t> bytes,
    const std::size_t offset) {
    if (offset > bytes.size() || sizeof(Integer) > bytes.size() - offset) {
        fail("durable format field extends past end of file");
    }
    using Unsigned = std::make_unsigned_t<Integer>;
    Unsigned value = 0;
    for (std::size_t i = 0; i < sizeof(Integer); ++i) {
        value |= static_cast<Unsigned>(bytes[offset + i]) << (8U * i);
    }
    return static_cast<Integer>(value);
}

bool checked_add(
    const std::uint64_t left,
    const std::uint64_t right,
    std::uint64_t& result) {
    if (right > std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    result = left + right;
    return true;
}

bool checked_multiply(
    const std::uint64_t left,
    const std::uint64_t right,
    std::uint64_t& result) {
    if (left != 0 && right > std::numeric_limits<std::uint64_t>::max() / left) {
        return false;
    }
    result = left * right;
    return true;
}

class Crc32cAccumulator {
public:
    void update(const std::span<const std::uint8_t> bytes) noexcept {
        for (const auto byte : bytes) {
            value_ ^= byte;
            for (int bit = 0; bit < 8; ++bit) {
                const auto mask =
                    static_cast<std::uint32_t>(0U - (value_ & 1U));
                value_ = (value_ >> 1U) ^ (0x82f63b78U & mask);
            }
        }
    }

    [[nodiscard]] std::uint32_t finish() const noexcept {
        return value_ ^ 0xffffffffU;
    }

private:
    std::uint32_t value_{0xffffffffU};
};

class DurableFile {
public:
#ifdef _WIN32
    using Native = HANDLE;
    static constexpr Native invalid = INVALID_HANDLE_VALUE;
#else
    using Native = int;
    static constexpr Native invalid = -1;
#endif

    DurableFile() = default;
    explicit DurableFile(const Native native) noexcept : native_(native) {}
    ~DurableFile() {
        close_noexcept();
    }

    DurableFile(const DurableFile&) = delete;
    DurableFile& operator=(const DurableFile&) = delete;

    DurableFile(DurableFile&& other) noexcept
        : native_(std::exchange(other.native_, invalid)) {}

    DurableFile& operator=(DurableFile&& other) noexcept {
        if (this != &other) {
            close_noexcept();
            native_ = std::exchange(other.native_, invalid);
        }
        return *this;
    }

    static DurableFile create_new(const std::filesystem::path& path) {
#ifdef _WIN32
        const auto handle = CreateFileW(
            path.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
        if (handle == invalid) {
            fail(system_message(
                "create " + path_text(path),
                static_cast<int>(GetLastError())));
        }
        return DurableFile(handle);
#else
        int descriptor;
        do {
            descriptor = ::open(
                path.c_str(),
                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                0600);
        } while (descriptor < 0 && errno == EINTR);
        if (descriptor < 0) {
            fail(system_message("create " + path_text(path), errno));
        }
        return DurableFile(descriptor);
#endif
    }

    static DurableFile open_read(const std::filesystem::path& path) {
#ifdef _WIN32
        const auto handle = CreateFileW(
            path.c_str(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
        if (handle == invalid) {
            fail(system_message(
                "open " + path_text(path),
                static_cast<int>(GetLastError())));
        }
        return DurableFile(handle);
#else
        int descriptor;
        do {
            descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        } while (descriptor < 0 && errno == EINTR);
        if (descriptor < 0) {
            fail(system_message("open " + path_text(path), errno));
        }
        return DurableFile(descriptor);
#endif
    }

    static DurableFile open_append(const std::filesystem::path& path) {
#ifdef _WIN32
        const auto handle = CreateFileW(
            path.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
        if (handle == invalid) {
            fail(system_message(
                "open " + path_text(path),
                static_cast<int>(GetLastError())));
        }
        DurableFile result(handle);
        LARGE_INTEGER end{};
        if (!SetFilePointerEx(handle, end, nullptr, FILE_END)) {
            fail(system_message(
                "seek " + path_text(path),
                static_cast<int>(GetLastError())));
        }
        return result;
#else
        int descriptor;
        do {
            descriptor = ::open(path.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
        } while (descriptor < 0 && errno == EINTR);
        if (descriptor < 0) {
            fail(system_message("open " + path_text(path), errno));
        }
        return DurableFile(descriptor);
#endif
    }

    void write_all(const std::span<const std::uint8_t> bytes) {
        std::size_t written = 0;
        while (written < bytes.size()) {
#ifdef _WIN32
            const auto chunk = static_cast<DWORD>(std::min<std::size_t>(
                bytes.size() - written,
                std::numeric_limits<DWORD>::max()));
            DWORD count = 0;
            if (!WriteFile(
                    native_,
                    bytes.data() + written,
                    chunk,
                    &count,
                    nullptr)) {
                fail(system_message(
                    "write durable file",
                    static_cast<int>(GetLastError())));
            }
            if (count == 0) {
                fail("write durable file made no progress");
            }
            written += count;
#else
            const auto count = ::write(
                native_,
                bytes.data() + written,
                bytes.size() - written);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                fail(system_message("write durable file", errno));
            }
            if (count == 0) {
                fail("write durable file made no progress");
            }
            written += static_cast<std::size_t>(count);
#endif
        }
    }

    void read_all(const std::span<std::uint8_t> bytes) {
        std::size_t read = 0;
        while (read < bytes.size()) {
#ifdef _WIN32
            const auto chunk = static_cast<DWORD>(std::min<std::size_t>(
                bytes.size() - read,
                std::numeric_limits<DWORD>::max()));
            DWORD count = 0;
            if (!ReadFile(native_, bytes.data() + read, chunk, &count, nullptr)) {
                fail(system_message(
                    "read durable file",
                    static_cast<int>(GetLastError())));
            }
            if (count == 0) {
                fail("durable file became shorter while reading");
            }
            read += count;
#else
            const auto count =
                ::read(native_, bytes.data() + read, bytes.size() - read);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                fail(system_message("read durable file", errno));
            }
            if (count == 0) {
                fail("durable file became shorter while reading");
            }
            read += static_cast<std::size_t>(count);
#endif
        }
    }

    [[nodiscard]] std::uint64_t size() const {
#ifdef _WIN32
        LARGE_INTEGER value{};
        if (!GetFileSizeEx(native_, &value) || value.QuadPart < 0) {
            fail(system_message(
                "measure durable file",
                static_cast<int>(GetLastError())));
        }
        return static_cast<std::uint64_t>(value.QuadPart);
#else
        struct stat status {};
        if (::fstat(native_, &status) != 0 || status.st_size < 0) {
            fail(system_message("measure durable file", errno));
        }
        return static_cast<std::uint64_t>(status.st_size);
#endif
    }

    void truncate(const std::uint64_t length) {
#ifdef _WIN32
        if (length >
            static_cast<std::uint64_t>(
                std::numeric_limits<LONGLONG>::max())) {
            fail("truncate length exceeds Windows file limit");
        }
        LARGE_INTEGER position{};
        position.QuadPart = static_cast<LONGLONG>(length);
        if (!SetFilePointerEx(native_, position, nullptr, FILE_BEGIN) ||
            !SetEndOfFile(native_)) {
            fail(system_message(
                "truncate durable file",
                static_cast<int>(GetLastError())));
        }
#else
        if (length >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            fail("truncate length exceeds POSIX file limit");
        }
        if (::ftruncate(native_, static_cast<off_t>(length)) != 0) {
            fail(system_message("truncate durable file", errno));
        }
#endif
    }

    void synchronize() {
#ifdef _WIN32
        if (!FlushFileBuffers(native_)) {
            fail(system_message(
                "synchronize durable file",
                static_cast<int>(GetLastError())));
        }
#else
        int result;
        do {
            result = ::fsync(native_);
        } while (result != 0 && errno == EINTR);
        if (result != 0) {
            fail(system_message("synchronize durable file", errno));
        }
#endif
    }

    void close() {
        if (native_ == invalid) {
            return;
        }
#ifdef _WIN32
        if (!CloseHandle(native_)) {
            native_ = invalid;
            fail(system_message(
                "close durable file",
                static_cast<int>(GetLastError())));
        }
#else
        const auto descriptor = std::exchange(native_, invalid);
        const auto result = ::close(descriptor);
        if (result != 0 && errno != EINTR) {
            fail(system_message("close durable file", errno));
        }
        return;
#endif
        native_ = invalid;
    }

private:
    void close_noexcept() noexcept {
        if (native_ == invalid) {
            return;
        }
#ifdef _WIN32
        CloseHandle(native_);
#else
        ::close(native_);
#endif
        native_ = invalid;
    }

    Native native_{invalid};
};

std::vector<std::uint8_t> read_file(
    const std::filesystem::path& path,
    const std::uint64_t maximum) {
    auto file = DurableFile::open_read(path);
    const auto length = file.size();
    if (length > maximum ||
        length > static_cast<std::uint64_t>(
                     std::numeric_limits<std::size_t>::max())) {
        fail("file exceeds configured size limit: " + path_text(path));
    }
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
    file.read_all(bytes);
    return bytes;
}

void synchronize_directory_best_effort(
    const std::filesystem::path& directory) {
#ifdef _WIN32
    const auto handle = CreateFileW(
        directory.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const auto error = GetLastError();
        if (error == ERROR_ACCESS_DENIED ||
            error == ERROR_INVALID_HANDLE ||
            error == ERROR_NOT_SUPPORTED) {
            return;
        }
        fail(system_message(
            "open directory for synchronization",
            static_cast<int>(error)));
    }
    if (!FlushFileBuffers(handle)) {
        const auto error = GetLastError();
        CloseHandle(handle);
        if (error == ERROR_ACCESS_DENIED ||
            error == ERROR_INVALID_HANDLE ||
            error == ERROR_NOT_SUPPORTED) {
            return;
        }
        fail(system_message(
            "synchronize directory",
            static_cast<int>(error)));
    }
    CloseHandle(handle);
#else
    int descriptor;
    do {
        descriptor = ::open(
            directory.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        if (errno == EINVAL || errno == ENOTSUP) {
            return;
        }
        fail(system_message("open directory for synchronization", errno));
    }
    int result;
    do {
        result = ::fsync(descriptor);
    } while (result != 0 && errno == EINTR);
    const auto synchronization_error = errno;
    ::close(descriptor);
    if (result != 0 &&
        synchronization_error != EINVAL &&
        synchronization_error != ENOTSUP) {
        fail(system_message(
            "synchronize directory",
            synchronization_error));
    }
#endif
}

void rename_no_replace(
    const std::filesystem::path& temporary,
    const std::filesystem::path& final_path) {
#ifdef _WIN32
    if (!MoveFileExW(
            temporary.c_str(),
            final_path.c_str(),
            MOVEFILE_WRITE_THROUGH)) {
        fail(system_message(
            "publish " + path_text(final_path),
            static_cast<int>(GetLastError())));
    }
#else
    if (::link(temporary.c_str(), final_path.c_str()) != 0) {
        fail(system_message("publish " + path_text(final_path), errno));
    }
    if (::unlink(temporary.c_str()) != 0) {
        fail(system_message("remove publication temporary file", errno));
    }
#endif
}

std::atomic<std::uint64_t> unique_counter{0};

std::string unique_suffix() {
    const auto clock = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto count = unique_counter.fetch_add(1, std::memory_order_relaxed);
    std::ostringstream text;
    text << std::hex << clock << '-' << count;
    return text.str();
}

std::string decimal_name(
    const std::string& prefix,
    const std::uint64_t value,
    const std::string& suffix) {
    std::ostringstream name;
    name << prefix << std::setw(20) << std::setfill('0') << value << suffix;
    return name.str();
}

std::optional<std::uint64_t> parse_decimal_name(
    const std::string& name,
    const std::string& prefix,
    const std::string& suffix) {
    constexpr std::size_t digits = 20;
    if (name.size() != prefix.size() + digits + suffix.size() ||
        !name.starts_with(prefix) || !name.ends_with(suffix)) {
        return std::nullopt;
    }
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < digits; ++i) {
        const auto character = name[prefix.size() + i];
        if (character < '0' || character > '9') {
            return std::nullopt;
        }
        const auto digit = static_cast<std::uint64_t>(character - '0');
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
            return std::nullopt;
        }
        value = value * 10U + digit;
    }
    return value;
}

std::vector<std::uint8_t> make_segment_header(
    const std::uint64_t sequence,
    const std::uint64_t first_index) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(wal_segment_header_size);
    bytes.insert(bytes.end(), wal_magic.begin(), wal_magic.end());
    append_le(bytes, storage_format_version);
    append_le(bytes, static_cast<std::uint16_t>(wal_segment_header_size));
    append_le(bytes, std::uint32_t{0});
    append_le(bytes, sequence);
    append_le(bytes, first_index);
    append_le(bytes, crc32c(bytes));
    append_le(bytes, std::uint32_t{0});
    return bytes;
}

std::vector<std::uint8_t> encode_record(const WalEntry& entry) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(wal_record_header_size + entry.payload.size());
    bytes.insert(bytes.end(), record_magic.begin(), record_magic.end());
    append_le(bytes, storage_format_version);
    append_le(bytes, static_cast<std::uint16_t>(entry.type));
    append_le(bytes, static_cast<std::uint32_t>(wal_record_header_size));
    append_le(bytes, entry.term.value);
    append_le(bytes, entry.index.value);
    append_le(bytes, static_cast<std::uint64_t>(entry.payload.size()));
    Crc32cAccumulator checksum;
    checksum.update(bytes);
    checksum.update(entry.payload);
    append_le(bytes, checksum.finish());
    bytes.insert(bytes.end(), entry.payload.begin(), entry.payload.end());
    return bytes;
}

struct SegmentInfo {
    std::filesystem::path path;
    std::uint64_t sequence{};
    std::uint64_t first_index{};
    std::optional<std::uint64_t> maximum_index;
    std::uint64_t size{};
};

void remove_specific(const std::filesystem::path& path) {
    std::error_code error;
    const auto removed = std::filesystem::remove(path, error);
    if (error || !removed) {
        fail(
            "remove " + path_text(path) + ": " +
            (error ? error.message() : "file was not present"));
    }
}

void validate_snapshot_value(
    const Snapshot& snapshot,
    const StorageLimits& limits) {
    const auto& metadata = snapshot.metadata;
    if (metadata.format_version != storage_format_version) {
        fail("snapshot format_version must be 1");
    }
    if (metadata.last_included_index.value == 0) {
        fail("snapshot last-included index must be nonzero");
    }
    if (metadata.store_revision.value < 0 ||
        metadata.compaction_revision.value < 0 ||
        metadata.compaction_revision.value > metadata.store_revision.value) {
        fail("snapshot revisions are invalid");
    }
    const auto voter_count = metadata.membership.voters.size();
    const auto learner_count = metadata.membership.learners.size();
    if (voter_count > limits.max_snapshot_members ||
        learner_count > limits.max_snapshot_members ||
        voter_count >
            static_cast<std::size_t>(
                limits.max_snapshot_members - learner_count)) {
        fail("snapshot membership exceeds configured limit");
    }
    std::unordered_set<std::uint64_t> members;
    members.reserve(voter_count + learner_count);
    for (const auto node : metadata.membership.voters) {
        if (node.value == 0 || !members.insert(node.value).second) {
            fail("snapshot membership contains zero or duplicate node ID");
        }
    }
    for (const auto node : metadata.membership.learners) {
        if (node.value == 0 || !members.insert(node.value).second) {
            fail("snapshot membership contains zero or duplicate node ID");
        }
    }
    std::uint64_t member_bytes = 0;
    std::uint64_t total = snapshot_header_size;
    if (!checked_multiply(
            static_cast<std::uint64_t>(members.size()),
            sizeof(std::uint64_t),
            member_bytes) ||
        !checked_add(total, member_bytes, total) ||
        !checked_add(
            total,
            static_cast<std::uint64_t>(snapshot.state.size()),
            total) ||
        total > limits.max_snapshot_bytes) {
        fail("snapshot exceeds configured size limit");
    }
}

std::vector<std::uint8_t> encode_snapshot(
    const Snapshot& snapshot,
    const StorageLimits& limits) {
    validate_snapshot_value(snapshot, limits);
    std::vector<std::uint8_t> body;
    const auto member_count =
        snapshot.metadata.membership.voters.size() +
        snapshot.metadata.membership.learners.size();
    const auto body_size =
        static_cast<std::uint64_t>(member_count) * sizeof(std::uint64_t) +
        snapshot.state.size();
    body.reserve(static_cast<std::size_t>(body_size));
    for (const auto node : snapshot.metadata.membership.voters) {
        append_le(body, node.value);
    }
    for (const auto node : snapshot.metadata.membership.learners) {
        append_le(body, node.value);
    }
    body.insert(body.end(), snapshot.state.begin(), snapshot.state.end());

    std::vector<std::uint8_t> bytes;
    bytes.reserve(snapshot_header_size + body.size());
    bytes.insert(bytes.end(), snapshot_magic.begin(), snapshot_magic.end());
    append_le(bytes, storage_format_version);
    append_le(bytes, static_cast<std::uint16_t>(snapshot_header_size));
    append_le(bytes, std::uint32_t{0});
    append_le(bytes, snapshot.metadata.last_included_index.value);
    append_le(bytes, snapshot.metadata.last_included_term.value);
    append_le(bytes, snapshot.metadata.store_revision.value);
    append_le(bytes, snapshot.metadata.compaction_revision.value);
    append_le(
        bytes,
        static_cast<std::uint32_t>(
            snapshot.metadata.membership.voters.size()));
    append_le(
        bytes,
        static_cast<std::uint32_t>(
            snapshot.metadata.membership.learners.size()));
    append_le(bytes, static_cast<std::uint64_t>(snapshot.state.size()));
    Crc32cAccumulator checksum;
    checksum.update(bytes);
    checksum.update(body);
    append_le(bytes, checksum.finish());
    bytes.insert(bytes.end(), body.begin(), body.end());
    return bytes;
}

Snapshot decode_snapshot(
    const std::filesystem::path& path,
    const StorageLimits& limits) {
    const auto bytes = read_file(path, limits.max_snapshot_bytes);
    if (bytes.size() < snapshot_header_size) {
        fail("snapshot header is truncated: " + path_text(path));
    }
    const std::span<const std::uint8_t> view(bytes);
    if (!std::equal(snapshot_magic.begin(), snapshot_magic.end(), view.begin())) {
        fail("snapshot magic is invalid: " + path_text(path));
    }
    if (read_le<std::uint16_t>(view, 8) != storage_format_version ||
        read_le<std::uint16_t>(view, 10) != snapshot_header_size ||
        read_le<std::uint32_t>(view, 12) != 0) {
        fail("snapshot version, header size, or flags are unsupported");
    }
    const auto index = read_le<std::uint64_t>(view, 16);
    const auto term = read_le<std::uint64_t>(view, 24);
    const auto store_revision_bits = read_le<std::uint64_t>(view, 32);
    const auto compaction_revision_bits = read_le<std::uint64_t>(view, 40);
    const auto voter_count = read_le<std::uint32_t>(view, 48);
    const auto learner_count = read_le<std::uint32_t>(view, 52);
    const auto state_length = read_le<std::uint64_t>(view, 56);
    const auto stored_checksum = read_le<std::uint32_t>(view, 64);
    if ((store_revision_bits >> 63U) != 0 ||
        (compaction_revision_bits >> 63U) != 0) {
        fail("snapshot revision is negative");
    }
    if (voter_count > limits.max_snapshot_members ||
        learner_count > limits.max_snapshot_members ||
        voter_count > limits.max_snapshot_members - learner_count) {
        fail("snapshot membership exceeds configured limit");
    }
    std::uint64_t member_count = 0;
    std::uint64_t member_bytes = 0;
    std::uint64_t expected = snapshot_header_size;
    if (!checked_add(voter_count, learner_count, member_count) ||
        !checked_multiply(member_count, sizeof(std::uint64_t), member_bytes) ||
        !checked_add(expected, member_bytes, expected) ||
        !checked_add(expected, state_length, expected) ||
        expected != bytes.size()) {
        fail("snapshot body length is invalid");
    }
    Crc32cAccumulator checksum;
    checksum.update(view.first(64));
    checksum.update(view.subspan(snapshot_header_size));
    if (checksum.finish() != stored_checksum) {
        fail("snapshot checksum mismatch: " + path_text(path));
    }

    Snapshot snapshot;
    snapshot.metadata.last_included_index.value = index;
    snapshot.metadata.last_included_term.value = term;
    snapshot.metadata.store_revision.value =
        static_cast<std::int64_t>(store_revision_bits);
    snapshot.metadata.compaction_revision.value =
        static_cast<std::int64_t>(compaction_revision_bits);
    snapshot.metadata.format_version = storage_format_version;
    snapshot.checksum = stored_checksum;
    std::size_t offset = snapshot_header_size;
    std::unordered_set<std::uint64_t> members;
    members.reserve(static_cast<std::size_t>(member_count));
    for (std::uint32_t i = 0; i < voter_count; ++i) {
        const auto node = read_le<std::uint64_t>(view, offset);
        offset += sizeof(std::uint64_t);
        if (node == 0 || !members.insert(node).second) {
            fail("snapshot membership contains zero or duplicate node ID");
        }
        snapshot.metadata.membership.voters.push_back({node});
    }
    for (std::uint32_t i = 0; i < learner_count; ++i) {
        const auto node = read_le<std::uint64_t>(view, offset);
        offset += sizeof(std::uint64_t);
        if (node == 0 || !members.insert(node).second) {
            fail("snapshot membership contains zero or duplicate node ID");
        }
        snapshot.metadata.membership.learners.push_back({node});
    }
    snapshot.state.assign(view.begin() + static_cast<std::ptrdiff_t>(offset), view.end());
    validate_snapshot_value(snapshot, limits);
    return snapshot;
}

}  // namespace

std::uint32_t crc32c(
    const std::span<const std::uint8_t> bytes) noexcept {
    Crc32cAccumulator checksum;
    checksum.update(bytes);
    return checksum.finish();
}

class SegmentedWriteAheadLog::Impl {
public:
    Impl(std::filesystem::path directory, StorageLimits limits)
        : directory_(
              std::filesystem::absolute(std::move(directory)).lexically_normal()),
          limits_(limits) {
        if (limits_.wal_segment_bytes <
                wal_segment_header_size + wal_record_header_size ||
            limits_.max_wal_payload_bytes >
                limits_.wal_segment_bytes -
                    wal_segment_header_size - wal_record_header_size ||
            limits_.max_recovery_records == 0) {
            fail("WAL storage limits are invalid");
        }
        std::error_code error;
        std::filesystem::create_directories(directory_, error);
        if (error) {
            fail("create WAL directory: " + error.message());
        }
        std::scoped_lock lock(mutex_);
        recover_locked();
    }

    void append(
        const std::span<const WalEntry> entries,
        const Durability durability) {
        std::scoped_lock lock(mutex_);
        if (!healthy_) {
            fail("WAL requires recover() after a previous append failure");
        }
        if (entries.empty()) {
            return;
        }
        validate_append(entries);
        try {
            for (const auto& entry : entries) {
                const auto encoded = encode_record(entry);
                ensure_segment(entry.index.value, encoded.size());
                active_file_->write_all(encoded);
                active_size_ += encoded.size();
                segments_.back().size = active_size_;
                segments_.back().maximum_index = entry.index.value;
                last_index_ = entry.index.value;
                ++recovered_record_count_;
                expected_append_ =
                    entry.index.value == std::numeric_limits<std::uint64_t>::max()
                    ? std::nullopt
                    : std::optional<std::uint64_t>(entry.index.value + 1U);
            }
            if (durability == Durability::synchronize) {
                active_file_->synchronize();
            }
        } catch (...) {
            healthy_ = false;
            active_file_.reset();
            throw;
        }
    }

    std::vector<WalEntry> recover() {
        std::scoped_lock lock(mutex_);
        return recover_locked();
    }

    void truncate_through(
        const LogIndex index,
        const SnapshotStore& snapshots) {
        std::scoped_lock lock(mutex_);
        if (index.value == 0) {
            fail("WAL truncation index must be nonzero");
        }
        const auto snapshot = snapshots.latest();
        if (!snapshot ||
            snapshot->metadata.last_included_index.value < index.value) {
            fail("WAL truncation is not covered by a durable snapshot");
        }
        recover_locked();
        if (segments_.size() < 2) {
            return;
        }
        std::vector<std::filesystem::path> removals;
        for (std::size_t i = 0; i + 1 < segments_.size(); ++i) {
            const auto& segment = segments_[i];
            if (segment.maximum_index &&
                *segment.maximum_index <= index.value) {
                removals.push_back(segment.path);
            }
        }
        active_file_.reset();
        try {
            for (const auto& path : removals) {
                remove_specific(path);
                synchronize_directory_best_effort(directory_);
            }
            recover_locked();
        } catch (...) {
            active_file_.reset();
            healthy_ = false;
            throw;
        }
    }

private:
    void validate_append(const std::span<const WalEntry> entries) const {
        if (entries.size() >
            limits_.max_recovery_records - std::min(
                limits_.max_recovery_records,
                recovered_record_count_)) {
            fail("WAL record count exceeds configured limit");
        }
        auto expected = expected_append_;
        if (!expected && last_index_) {
            fail("WAL index would overflow");
        }
        if (!expected) {
            expected = entries.front().index.value;
        }
        for (const auto& entry : entries) {
            if (!expected) {
                fail("WAL index would overflow");
            }
            if (entry.type != WalRecordType::command) {
                fail("unsupported WAL record type");
            }
            if (entry.index.value == 0 || entry.index.value != *expected) {
                fail("WAL append index is not the expected next index");
            }
            if (entry.payload.size() > limits_.max_wal_payload_bytes ||
                wal_record_header_size + entry.payload.size() >
                    limits_.wal_segment_bytes - wal_segment_header_size) {
                fail("WAL payload exceeds configured limit");
            }
            if (entry.index.value == std::numeric_limits<std::uint64_t>::max()) {
                expected.reset();
            } else {
                expected = entry.index.value + 1U;
            }
        }
    }

    void ensure_segment(
        const std::uint64_t first_index,
        const std::size_t record_size) {
        if (!active_file_) {
            if (!segments_.empty() &&
                segments_.back().sequence ==
                    std::numeric_limits<std::uint64_t>::max()) {
                fail("WAL segment sequence would overflow");
            }
            const auto sequence =
                segments_.empty() ? 1U : segments_.back().sequence + 1U;
            create_segment(sequence, first_index);
            return;
        }
        if (record_size <= limits_.wal_segment_bytes - active_size_) {
            return;
        }
        active_file_->synchronize();
        active_file_.reset();
        if (segments_.back().sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
            fail("WAL segment sequence would overflow");
        }
        create_segment(segments_.back().sequence + 1U, first_index);
    }

    void create_segment(
        const std::uint64_t sequence,
        const std::uint64_t first_index) {
        if (sequence == 0 || first_index == 0) {
            fail("WAL segment sequence and first index must be nonzero");
        }
        const auto final_path =
            directory_ /
            decimal_name("wal-", sequence, ".kwal");
        if (std::filesystem::exists(final_path)) {
            fail("refusing to overwrite existing WAL segment");
        }
        auto temporary = final_path;
        temporary += ".tmp.";
        temporary += unique_suffix();
        bool temporary_exists = false;
        try {
            auto file = DurableFile::create_new(temporary);
            temporary_exists = true;
            const auto header = make_segment_header(sequence, first_index);
            file.write_all(header);
            file.synchronize();
            file.close();
            rename_no_replace(temporary, final_path);
            temporary_exists = false;
            synchronize_directory_best_effort(directory_);
        } catch (...) {
            if (temporary_exists) {
                std::error_code ignored;
                std::filesystem::remove(temporary, ignored);
            }
            throw;
        }
        segments_.push_back({
            .path = final_path,
            .sequence = sequence,
            .first_index = first_index,
            .maximum_index = std::nullopt,
            .size = wal_segment_header_size});
        active_file_ = std::make_unique<DurableFile>(
            DurableFile::open_append(final_path));
        active_size_ = wal_segment_header_size;
        expected_append_ = first_index;
    }

    std::vector<std::filesystem::path> segment_paths() const {
        std::vector<std::pair<std::uint64_t, std::filesystem::path>> found;
        for (const auto& item : std::filesystem::directory_iterator(directory_)) {
            if (!item.is_regular_file()) {
                continue;
            }
            const auto name = item.path().filename().string();
            const auto sequence =
                parse_decimal_name(name, "wal-", ".kwal");
            if (sequence) {
                if (*sequence == 0) {
                    fail("WAL segment sequence zero is invalid");
                }
                found.emplace_back(*sequence, item.path());
            } else if (name.starts_with("wal-") && name.ends_with(".kwal")) {
                fail("malformed WAL segment filename: " + name);
            }
        }
        std::sort(
            found.begin(),
            found.end(),
            [](const auto& left, const auto& right) {
                return left.first < right.first;
            });
        for (std::size_t i = 1; i < found.size(); ++i) {
            if (found[i - 1].first ==
                    std::numeric_limits<std::uint64_t>::max() ||
                found[i].first != found[i - 1].first + 1U) {
                fail("WAL segment sequence gap or regression");
            }
        }
        std::vector<std::filesystem::path> paths;
        paths.reserve(found.size());
        for (auto& [sequence, path] : found) {
            static_cast<void>(sequence);
            paths.push_back(std::move(path));
        }
        return paths;
    }

    std::vector<WalEntry> recover_locked() {
        active_file_.reset();
        segments_.clear();
        last_index_.reset();
        expected_append_.reset();
        active_size_ = 0;
        recovered_record_count_ = 0;
        const auto paths = segment_paths();
        std::vector<WalEntry> entries;
        std::optional<std::uint64_t> expected;
        for (std::size_t segment_position = 0;
             segment_position < paths.size();
             ++segment_position) {
            const auto is_final = segment_position + 1 == paths.size();
            auto bytes = read_file(paths[segment_position], limits_.wal_segment_bytes);
            const std::span<const std::uint8_t> view(bytes);
            if (bytes.size() < wal_segment_header_size) {
                fail("WAL segment header is truncated: " +
                     path_text(paths[segment_position]));
            }
            if (!std::equal(wal_magic.begin(), wal_magic.end(), view.begin()) ||
                read_le<std::uint16_t>(view, 8) != storage_format_version ||
                read_le<std::uint16_t>(view, 10) != wal_segment_header_size ||
                read_le<std::uint32_t>(view, 12) != 0 ||
                read_le<std::uint32_t>(view, 36) != 0) {
                fail("WAL segment header is malformed");
            }
            const auto sequence = read_le<std::uint64_t>(view, 16);
            const auto first_index = read_le<std::uint64_t>(view, 24);
            if (sequence == 0 || first_index == 0 ||
                sequence != *parse_decimal_name(
                    paths[segment_position].filename().string(),
                    "wal-",
                    ".kwal") ||
                read_le<std::uint32_t>(view, 32) != crc32c(view.first(32))) {
                fail("WAL segment header checksum or identity is invalid");
            }
            if (expected && first_index != *expected) {
                fail("WAL segment first index has a gap or regression");
            }
            if (!expected) {
                expected = first_index;
            }

            SegmentInfo segment{
                .path = paths[segment_position],
                .sequence = sequence,
                .first_index = first_index,
                .maximum_index = std::nullopt,
                .size = static_cast<std::uint64_t>(bytes.size())};
            std::size_t offset = wal_segment_header_size;
            bool first_record = true;
            while (offset < bytes.size()) {
                const auto remaining = bytes.size() - offset;
                if (remaining < wal_record_header_size) {
                    if (!is_final) {
                        fail("truncated WAL record header in interior segment");
                    }
                    truncate_torn_suffix(segment.path, offset);
                    bytes.resize(offset);
                    segment.size = offset;
                    break;
                }
                const auto header = view.subspan(offset, wal_record_header_size);
                if (!std::equal(
                        record_magic.begin(),
                        record_magic.end(),
                        header.begin()) ||
                    read_le<std::uint16_t>(header, 4) != storage_format_version ||
                    read_le<std::uint16_t>(header, 6) != command_record_type ||
                    read_le<std::uint32_t>(header, 8) != wal_record_header_size) {
                    fail("WAL record header is malformed");
                }
                const auto term = read_le<std::uint64_t>(header, 12);
                const auto index = read_le<std::uint64_t>(header, 20);
                const auto payload_length = read_le<std::uint64_t>(header, 28);
                const auto stored_checksum = read_le<std::uint32_t>(header, 36);
                if (index == 0 || index != *expected) {
                    fail("WAL record index has a gap or regression");
                }
                if (first_record && index != first_index) {
                    fail("WAL segment first record does not match its header");
                }
                if (payload_length > limits_.max_wal_payload_bytes ||
                    payload_length >
                        limits_.wal_segment_bytes - wal_record_header_size) {
                    fail("WAL record payload length exceeds configured limit");
                }
                std::uint64_t record_size = 0;
                if (!checked_add(
                        wal_record_header_size,
                        payload_length,
                        record_size) ||
                    record_size >
                        std::numeric_limits<std::size_t>::max()) {
                    fail("WAL record length overflows");
                }
                if (record_size > remaining) {
                    if (!is_final) {
                        fail("truncated WAL record payload in interior segment");
                    }
                    truncate_torn_suffix(segment.path, offset);
                    bytes.resize(offset);
                    segment.size = offset;
                    break;
                }
                const auto payload = view.subspan(
                    offset + wal_record_header_size,
                    static_cast<std::size_t>(payload_length));
                Crc32cAccumulator checksum;
                checksum.update(header.first(36));
                checksum.update(payload);
                if (checksum.finish() != stored_checksum) {
                    fail("WAL record checksum mismatch");
                }
                if (recovered_record_count_ >= limits_.max_recovery_records) {
                    fail("WAL record count exceeds configured limit");
                }
                entries.push_back({
                    .type = WalRecordType::command,
                    .term = {term},
                    .index = {index},
                    .payload = std::vector<std::uint8_t>(
                        payload.begin(),
                        payload.end())});
                ++recovered_record_count_;
                segment.maximum_index = index;
                last_index_ = index;
                first_record = false;
                offset += static_cast<std::size_t>(record_size);
                if (index == std::numeric_limits<std::uint64_t>::max()) {
                    expected.reset();
                    if (offset != bytes.size() ||
                        segment_position + 1 != paths.size()) {
                        fail("WAL contains records after maximum index");
                    }
                } else {
                    expected = index + 1U;
                }
            }
            if (!segment.maximum_index && !is_final) {
                fail("empty WAL segment is not the final segment");
            }
            segments_.push_back(std::move(segment));
        }
        if (!segments_.empty()) {
            active_size_ = segments_.back().size;
            active_file_ = std::make_unique<DurableFile>(
                DurableFile::open_append(segments_.back().path));
            expected_append_ = expected;
        }
        healthy_ = true;
        return entries;
    }

    static void truncate_torn_suffix(
        const std::filesystem::path& path,
        const std::uint64_t offset) {
        auto file = DurableFile::open_append(path);
        file.truncate(offset);
        file.synchronize();
    }

    std::filesystem::path directory_;
    StorageLimits limits_;
    std::mutex mutex_;
    std::vector<SegmentInfo> segments_;
    std::unique_ptr<DurableFile> active_file_;
    std::optional<std::uint64_t> last_index_;
    std::optional<std::uint64_t> expected_append_;
    std::uint64_t active_size_{};
    std::size_t recovered_record_count_{};
    bool healthy_{true};
};

SegmentedWriteAheadLog::SegmentedWriteAheadLog(
    std::filesystem::path directory,
    StorageLimits limits)
    : impl_(std::make_unique<Impl>(std::move(directory), limits)) {}

SegmentedWriteAheadLog::~SegmentedWriteAheadLog() = default;

void SegmentedWriteAheadLog::append(
    const std::span<const WalEntry> entries,
    const Durability durability) {
    impl_->append(entries, durability);
}

std::vector<WalEntry> SegmentedWriteAheadLog::recover() {
    return impl_->recover();
}

void SegmentedWriteAheadLog::truncate_through(
    const LogIndex index,
    const SnapshotStore& snapshots) {
    impl_->truncate_through(index, snapshots);
}

class FileSnapshotStore::Impl {
public:
    Impl(std::filesystem::path directory, StorageLimits limits)
        : directory_(
              std::filesystem::absolute(std::move(directory)).lexically_normal()),
          limits_(limits) {
        if (limits_.max_snapshot_bytes < snapshot_header_size ||
            limits_.max_snapshot_members == 0) {
            fail("snapshot storage limits are invalid");
        }
        std::error_code error;
        std::filesystem::create_directories(directory_, error);
        if (error) {
            fail("create snapshot directory: " + error.message());
        }
    }

    void publish(const Snapshot& snapshot) {
        std::scoped_lock lock(mutex_);
        const auto current = latest_locked();
        if (current &&
            snapshot.metadata.last_included_index.value <=
                current->metadata.last_included_index.value) {
            fail("snapshot publication must advance the snapshot index");
        }
        const auto bytes = encode_snapshot(snapshot, limits_);
        const auto final_path =
            directory_ /
            decimal_name(
                "snapshot-",
                snapshot.metadata.last_included_index.value,
                ".ksnap");
        if (std::filesystem::exists(final_path)) {
            fail("refusing to replace an existing snapshot");
        }
        auto temporary = final_path;
        temporary += ".tmp.";
        temporary += unique_suffix();
        bool temporary_exists = false;
        try {
            auto file = DurableFile::create_new(temporary);
            temporary_exists = true;
            file.write_all(bytes);
            file.synchronize();
            file.close();
            rename_no_replace(temporary, final_path);
            temporary_exists = false;
            synchronize_directory_best_effort(directory_);
        } catch (...) {
            if (temporary_exists) {
                std::error_code ignored;
                std::filesystem::remove(temporary, ignored);
            }
            throw;
        }
    }

    std::optional<Snapshot> latest() const {
        std::scoped_lock lock(mutex_);
        return latest_locked();
    }

    Snapshot validate(const std::filesystem::path& path) const {
        std::scoped_lock lock(mutex_);
        const auto absolute =
            std::filesystem::absolute(path).lexically_normal();
        if (absolute.parent_path() != directory_ ||
            !parse_decimal_name(
                absolute.filename().string(),
                "snapshot-",
                ".ksnap")) {
            fail("snapshot validation path is outside the owned directory");
        }
        const auto name_index = *parse_decimal_name(
            absolute.filename().string(),
            "snapshot-",
            ".ksnap");
        auto snapshot = decode_snapshot(absolute, limits_);
        if (snapshot.metadata.last_included_index.value != name_index) {
            fail("snapshot filename and content index differ");
        }
        return snapshot;
    }

private:
    std::optional<Snapshot> latest_locked() const {
        std::vector<std::pair<std::uint64_t, std::filesystem::path>> candidates;
        for (const auto& item : std::filesystem::directory_iterator(directory_)) {
            if (!item.is_regular_file()) {
                continue;
            }
            const auto index = parse_decimal_name(
                item.path().filename().string(),
                "snapshot-",
                ".ksnap");
            if (index && *index != 0) {
                candidates.emplace_back(*index, item.path());
            }
        }
        std::sort(
            candidates.begin(),
            candidates.end(),
            [](const auto& left, const auto& right) {
                return left.first > right.first;
            });
        for (const auto& [index, path] : candidates) {
            try {
                auto snapshot = decode_snapshot(path, limits_);
                if (snapshot.metadata.last_included_index.value != index) {
                    continue;
                }
                return snapshot;
            } catch (const StorageError&) {
            }
        }
        return std::nullopt;
    }

    std::filesystem::path directory_;
    StorageLimits limits_;
    mutable std::mutex mutex_;
};

FileSnapshotStore::FileSnapshotStore(
    std::filesystem::path directory,
    StorageLimits limits)
    : impl_(std::make_unique<Impl>(std::move(directory), limits)) {}

FileSnapshotStore::~FileSnapshotStore() = default;

void FileSnapshotStore::publish(const Snapshot& snapshot) {
    impl_->publish(snapshot);
}

std::optional<Snapshot> FileSnapshotStore::latest() const {
    return impl_->latest();
}

Snapshot FileSnapshotStore::validate(
    const std::filesystem::path& path) const {
    return impl_->validate(path);
}

}  // namespace kura::metadata
