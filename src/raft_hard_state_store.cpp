#include "kura/metadata/storage/raft_hard_state_store.hpp"

#include "kura/metadata/storage/checksum.hpp"
#include "kura/metadata/storage/storage_error.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <mutex>
#include <span>
#include <sstream>
#include <string>
#include <system_error>
#include <type_traits>
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

constexpr std::size_t hard_state_record_size = 48;
constexpr std::uint16_t hard_state_format_version = 1;
constexpr std::array<std::uint8_t, 8> hard_state_magic{
    'K', 'U', 'R', 'A', 'H', 'S', 'T', '1'};
constexpr auto hard_state_name = "raft-hard-state.krhs";

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
        fail("Raft hard-state field extends past end of file");
    }
    using Unsigned = std::make_unsigned_t<Integer>;
    Unsigned value = 0;
    for (std::size_t i = 0; i < sizeof(Integer); ++i) {
        value |= static_cast<Unsigned>(bytes[offset + i]) << (8U * i);
    }
    return static_cast<Integer>(value);
}

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
                    "write Raft hard state",
                    static_cast<int>(GetLastError())));
            }
            if (count == 0) {
                fail("write Raft hard state made no progress");
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
                fail(system_message("write Raft hard state", errno));
            }
            if (count == 0) {
                fail("write Raft hard state made no progress");
            }
            written += static_cast<std::size_t>(count);
#endif
        }
    }

    void read_all(const std::span<std::uint8_t> bytes) {
        std::size_t read = 0;
        while (read < bytes.size()) {
#ifdef _WIN32
            DWORD count = 0;
            if (!ReadFile(
                    native_,
                    bytes.data() + read,
                    static_cast<DWORD>(bytes.size() - read),
                    &count,
                    nullptr)) {
                fail(system_message(
                    "read Raft hard state",
                    static_cast<int>(GetLastError())));
            }
            if (count == 0) {
                fail("Raft hard-state file became shorter while reading");
            }
            read += count;
#else
            const auto count =
                ::read(native_, bytes.data() + read, bytes.size() - read);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                fail(system_message("read Raft hard state", errno));
            }
            if (count == 0) {
                fail("Raft hard-state file became shorter while reading");
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
                "measure Raft hard state",
                static_cast<int>(GetLastError())));
        }
        return static_cast<std::uint64_t>(value.QuadPart);
#else
        struct stat status {};
        if (::fstat(native_, &status) != 0 || status.st_size < 0) {
            fail(system_message("measure Raft hard state", errno));
        }
        return static_cast<std::uint64_t>(status.st_size);
#endif
    }

    void synchronize() {
#ifdef _WIN32
        if (!FlushFileBuffers(native_)) {
            fail(system_message(
                "synchronize Raft hard state",
                static_cast<int>(GetLastError())));
        }
#else
        int result;
        do {
            result = ::fsync(native_);
        } while (result != 0 && errno == EINTR);
        if (result != 0) {
            fail(system_message("synchronize Raft hard state", errno));
        }
#endif
    }

    void close() {
        if (native_ == invalid) {
            return;
        }
#ifdef _WIN32
        const auto handle = std::exchange(native_, invalid);
        if (!CloseHandle(handle)) {
            fail(system_message(
                "close Raft hard state",
                static_cast<int>(GetLastError())));
        }
#else
        const auto descriptor = std::exchange(native_, invalid);
        const auto result = ::close(descriptor);
        if (result != 0 && errno != EINTR) {
            fail(system_message("close Raft hard state", errno));
        }
#endif
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
            "open hard-state directory for synchronization",
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
            "synchronize hard-state directory",
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
        fail(system_message(
            "open hard-state directory for synchronization",
            errno));
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
            "synchronize hard-state directory",
            synchronization_error));
    }
#endif
}

void replace_atomically(
    const std::filesystem::path& temporary,
    const std::filesystem::path& final_path) {
#ifdef _WIN32
    std::error_code exists_error;
    const auto exists = std::filesystem::exists(final_path, exists_error);
    if (exists_error) {
        fail(
            "inspect Raft hard-state destination: " +
            exists_error.message());
    }
    if (exists) {
        if (!ReplaceFileW(
                final_path.c_str(),
                temporary.c_str(),
                nullptr,
                REPLACEFILE_WRITE_THROUGH,
                nullptr,
                nullptr)) {
            fail(system_message(
                "replace " + path_text(final_path),
                static_cast<int>(GetLastError())));
        }
    } else if (!MoveFileExW(
                   temporary.c_str(),
                   final_path.c_str(),
                   MOVEFILE_WRITE_THROUGH)) {
        fail(system_message(
            "publish " + path_text(final_path),
            static_cast<int>(GetLastError())));
    }
#else
    if (::rename(temporary.c_str(), final_path.c_str()) != 0) {
        fail(system_message("replace " + path_text(final_path), errno));
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

std::vector<std::uint8_t> encode(
    const std::uint64_t generation,
    const RaftHardState& state) {
    if (generation == 0) {
        fail("Raft hard-state generation must be nonzero");
    }
    if (state.voted_for && state.voted_for->value == 0) {
        fail("Raft votedFor must be absent or a nonzero node ID");
    }
    if (state.current_term.value == 0 && state.voted_for) {
        fail("Raft term zero cannot contain a vote");
    }
    std::vector<std::uint8_t> bytes;
    bytes.reserve(hard_state_record_size);
    bytes.insert(bytes.end(), hard_state_magic.begin(), hard_state_magic.end());
    append_le(bytes, hard_state_format_version);
    append_le(bytes, static_cast<std::uint16_t>(hard_state_record_size));
    append_le(bytes, std::uint32_t{0});
    append_le(bytes, generation);
    append_le(bytes, state.current_term.value);
    append_le(bytes, state.voted_for ? state.voted_for->value : 0U);
    append_le(bytes, crc32c(bytes));
    append_le(bytes, std::uint32_t{0});
    return bytes;
}

struct DecodedHardState {
    std::uint64_t generation{};
    RaftHardState state;
};

DecodedHardState decode(
    const std::filesystem::path& path,
    const StorageLimits& limits) {
    auto file = DurableFile::open_read(path);
    const auto size = file.size();
    if (size > limits.max_raft_hard_state_bytes) {
        fail("Raft hard-state file exceeds configured size limit");
    }
    if (size != hard_state_record_size) {
        fail("Raft hard-state record is torn or has trailing bytes");
    }
    std::array<std::uint8_t, hard_state_record_size> bytes{};
    file.read_all(bytes);
    const std::span<const std::uint8_t> view(bytes);
    if (!std::equal(
            hard_state_magic.begin(),
            hard_state_magic.end(),
            view.begin())) {
        fail("Raft hard-state magic is invalid");
    }
    if (read_le<std::uint16_t>(view, 8) != hard_state_format_version ||
        read_le<std::uint16_t>(view, 10) != hard_state_record_size ||
        read_le<std::uint32_t>(view, 12) != 0 ||
        read_le<std::uint32_t>(view, 44) != 0) {
        fail("Raft hard-state version, size, flags, or reserved field is invalid");
    }
    const auto generation = read_le<std::uint64_t>(view, 16);
    const auto term = read_le<std::uint64_t>(view, 24);
    const auto voted_for = read_le<std::uint64_t>(view, 32);
    if (generation == 0) {
        fail("Raft hard-state generation zero is invalid");
    }
    if (term == 0 && voted_for != 0) {
        fail("Raft hard-state term zero contains a vote");
    }
    if (read_le<std::uint32_t>(view, 40) != crc32c(view.first(40))) {
        fail("Raft hard-state checksum mismatch");
    }
    return {
        .generation = generation,
        .state = {
            .current_term = {term},
            .voted_for = voted_for == 0
                ? std::nullopt
                : std::optional<NodeId>(NodeId{voted_for})}};
}

void validate_transition(
    const RaftHardState& current,
    const RaftHardState& next) {
    if (next.voted_for && next.voted_for->value == 0) {
        fail("Raft votedFor must be absent or a nonzero node ID");
    }
    if (next.current_term.value == 0 && next.voted_for) {
        fail("Raft term zero cannot contain a vote");
    }
    if (next.current_term < current.current_term) {
        fail("Raft currentTerm must not regress");
    }
    if (next.current_term == current.current_term &&
        current.voted_for != next.voted_for &&
        current.voted_for.has_value()) {
        fail("Raft votedFor must not change or clear within a term");
    }
}

}  // namespace

class FileRaftHardStateStore::Impl {
public:
    Impl(
        std::filesystem::path directory,
        StorageLimits limits,
        RaftHardStateFaultHook fault_hook)
        : directory_(
              std::filesystem::absolute(std::move(directory)).lexically_normal()),
          final_path_(directory_ / hard_state_name),
          limits_(limits),
          fault_hook_(std::move(fault_hook)) {
        if (limits_.max_raft_hard_state_bytes < hard_state_record_size) {
            fail("Raft hard-state storage limit is smaller than the v1 record");
        }
        std::error_code error;
        std::filesystem::create_directories(directory_, error);
        if (error) {
            fail("create Raft hard-state directory: " + error.message());
        }
        const auto decoded = load_decoded();
        state_ = decoded.state;
        generation_ = decoded.generation;
    }

    RaftHardState load() const {
        std::scoped_lock lock(mutex_);
        if (!healthy_) {
            fail("Raft hard-state store must be reopened after a write failure");
        }
        const auto decoded = load_decoded();
        if (decoded.generation != generation_ || decoded.state != state_) {
            fail("Raft hard state changed outside its owning store");
        }
        return state_;
    }

    RaftHardStatePersisted persist(const PersistRaftHardState& request) {
        std::scoped_lock lock(mutex_);
        if (!healthy_) {
            fail("Raft hard-state store must be reopened after a write failure");
        }
        if (request.request_id == 0) {
            fail("Raft hard-state persistence request ID must be nonzero");
        }
        validate_transition(state_, request.state);
        if (request.state == state_) {
            return {
                .request_id = request.request_id,
                .state = state_};
        }
        if (generation_ == std::numeric_limits<std::uint64_t>::max()) {
            fail("Raft hard-state generation would overflow");
        }

        const auto next_generation = generation_ + 1U;
        const auto bytes = encode(next_generation, request.state);
        auto temporary = final_path_;
        temporary += ".tmp.";
        temporary += unique_suffix();
        bool temporary_exists = false;
        try {
            auto file = DurableFile::create_new(temporary);
            temporary_exists = true;
            reached(RaftHardStateWriteBoundary::temporary_created);
            file.write_all(bytes);
            reached(RaftHardStateWriteBoundary::record_written);
            file.synchronize();
            reached(RaftHardStateWriteBoundary::temporary_synchronized);
            file.close();
            reached(RaftHardStateWriteBoundary::temporary_closed);
            replace_atomically(temporary, final_path_);
            temporary_exists = false;
            reached(RaftHardStateWriteBoundary::final_replaced);
            synchronize_directory_best_effort(directory_);
            reached(RaftHardStateWriteBoundary::directory_synchronized);
        } catch (...) {
            healthy_ = false;
            if (temporary_exists) {
                std::error_code ignored;
                std::filesystem::remove(temporary, ignored);
            }
            throw;
        }
        generation_ = next_generation;
        state_ = request.state;
        return {
            .request_id = request.request_id,
            .state = state_};
    }

private:
    DecodedHardState load_decoded() const {
        std::error_code error;
        const auto exists = std::filesystem::exists(final_path_, error);
        if (error) {
            fail("inspect Raft hard-state file: " + error.message());
        }
        if (!exists) {
            return {};
        }
        return decode(final_path_, limits_);
    }

    void reached(const RaftHardStateWriteBoundary boundary) const {
        if (fault_hook_) {
            fault_hook_(boundary);
        }
    }

    std::filesystem::path directory_;
    std::filesystem::path final_path_;
    StorageLimits limits_;
    RaftHardStateFaultHook fault_hook_;
    mutable std::mutex mutex_;
    RaftHardState state_;
    std::uint64_t generation_{};
    bool healthy_{true};
};

FileRaftHardStateStore::FileRaftHardStateStore(
    std::filesystem::path directory,
    StorageLimits limits,
    RaftHardStateFaultHook fault_hook)
    : impl_(std::make_unique<Impl>(
          std::move(directory),
          limits,
          std::move(fault_hook))) {}

FileRaftHardStateStore::~FileRaftHardStateStore() = default;

RaftHardState FileRaftHardStateStore::load() const {
    return impl_->load();
}

RaftHardStatePersisted FileRaftHardStateStore::persist(
    const PersistRaftHardState& request) {
    return impl_->persist(request);
}

}  // namespace kura::metadata
