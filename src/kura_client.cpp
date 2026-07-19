#include "kura/metadata/kura/kura_client.hpp"

#include "kura/metadata/core/key_range.hpp"
#include "kura/metadata/core/store_error.hpp"
#include "kura/metadata/in_memory_metadata_store.hpp"
#include "kura/metadata/kv/compare_result.hpp"
#include "kura/metadata/kv/compare_target.hpp"
#include "kura/metadata/kv/delete_request.hpp"
#include "kura/metadata/kv/mutation_event.hpp"
#include "kura/metadata/kv/put_request.hpp"
#include "kura/metadata/kv/range_request.hpp"
#include "kura/metadata/lease/lease_request.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <span>
#include <thread>
#include <utility>

namespace kura::metadata {
namespace {

constexpr std::string_view pointer_magic{"KSP1"};
std::atomic<std::uint64_t> next_client_id{1};

void validate_identifier(
    const std::string_view value,
    const std::string_view kind) {
    if (value.empty()
        || !std::ranges::all_of(value, [](const unsigned char character) {
               return (character >= 'a' && character <= 'z')
                   || (character >= 'A' && character <= 'Z')
                   || (character >= '0' && character <= '9')
                   || character == '-' || character == '_'
                   || character == '.';
           })) {
        throw std::invalid_argument(
            std::string(kind) + " must use [A-Za-z0-9._-]");
    }
}

void append_u32(
    std::vector<std::uint8_t>& output,
    const std::size_t value) {
    if (value > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("snapshot pointer field is too large");
    }
    const auto narrowed = static_cast<std::uint32_t>(value);
    for (unsigned shift = 0; shift != 32; shift += 8) {
        output.push_back(
            static_cast<std::uint8_t>((narrowed >> shift) & 0xffU));
    }
}

void append_field(
    std::vector<std::uint8_t>& output,
    const std::span<const std::uint8_t> value) {
    append_u32(output, value.size());
    output.insert(output.end(), value.begin(), value.end());
}

void append_field(
    std::vector<std::uint8_t>& output,
    const std::string_view value) {
    append_field(
        output,
        std::span{
            reinterpret_cast<const std::uint8_t*>(value.data()),
            value.size()});
}

ByteSequence encode_pointer(const SnapshotPointer& pointer) {
    validate_identifier(pointer.snapshot_id, "snapshot ID");
    validate_identifier(pointer.schema_id, "schema ID");
    if (pointer.manifest_uri.empty()) {
        throw std::invalid_argument("manifest URI must not be empty");
    }
    if (pointer.integrity_hash.empty()) {
        throw std::invalid_argument("integrity hash must not be empty");
    }

    std::vector<std::uint8_t> encoded(
        pointer_magic.begin(),
        pointer_magic.end());
    append_field(encoded, pointer.snapshot_id);
    append_field(encoded, pointer.manifest_uri);
    append_field(encoded, pointer.schema_id);
    append_field(encoded, std::span{pointer.integrity_hash});
    return ByteSequence{std::move(encoded)};
}

std::uint32_t read_u32(
    const std::span<const std::uint8_t> input,
    std::size_t& offset) {
    if (input.size() - std::min(input.size(), offset) < 4) {
        throw KuraClientError(
            StatusCode::corruption,
            "truncated snapshot pointer length");
    }
    std::uint32_t value{};
    for (unsigned shift = 0; shift != 32; shift += 8) {
        value |= static_cast<std::uint32_t>(input[offset++]) << shift;
    }
    return value;
}

std::span<const std::uint8_t> read_field(
    const std::span<const std::uint8_t> input,
    std::size_t& offset) {
    const auto size = read_u32(input, offset);
    if (size > input.size() - std::min(input.size(), offset)) {
        throw KuraClientError(
            StatusCode::corruption,
            "truncated snapshot pointer field");
    }
    const auto result = input.subspan(offset, size);
    offset += size;
    return result;
}

std::string field_string(const std::span<const std::uint8_t> value) {
    return {
        reinterpret_cast<const char*>(value.data()),
        value.size()};
}

SnapshotPointer decode_pointer(const ByteSequence& encoded) {
    const auto input = encoded.bytes();
    if (input.size() < pointer_magic.size()
        || !std::equal(
            pointer_magic.begin(),
            pointer_magic.end(),
            input.begin())) {
        throw KuraClientError(
            StatusCode::corruption,
            "snapshot pointer has an invalid format marker");
    }
    std::size_t offset = pointer_magic.size();
    SnapshotPointer pointer{
        .snapshot_id = field_string(read_field(input, offset)),
        .manifest_uri = field_string(read_field(input, offset)),
        .schema_id = field_string(read_field(input, offset))};
    const auto hash = read_field(input, offset);
    pointer.integrity_hash.assign(hash.begin(), hash.end());
    if (offset != input.size()) {
        throw KuraClientError(
            StatusCode::corruption,
            "snapshot pointer has trailing bytes");
    }
    return pointer;
}

ByteSequence bytes(const std::string_view value) {
    return ByteSequence::from_string(value);
}

ByteSequence exact_key_end(const ByteSequence& key) {
    std::vector<std::uint8_t> end(key.bytes().begin(), key.bytes().end());
    end.push_back(0);
    return ByteSequence{std::move(end)};
}

Compare numeric_compare(
    const ByteSequence& key,
    const CompareTarget target,
    const CompareResult result,
    const std::int64_t expected) {
    return {
        .key = key,
        .target = target,
        .result = result,
        .expected = expected};
}

class SteadyTickSource final
    : public InProcessKuraMetadataBackend::TickSource {
public:
    [[nodiscard]] LeaseTick current_tick() const override {
        const auto elapsed =
            std::chrono::steady_clock::now() - started_;
        return LeaseTick{static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(elapsed)
                .count())};
    }

private:
    std::chrono::steady_clock::time_point started_{
        std::chrono::steady_clock::now()};
};

struct LeaseSession {
    std::shared_ptr<KuraMetadataBackend> backend;
    LeaseId id;
    FencingToken token;
    std::chrono::milliseconds keepalive_interval;
    std::atomic<bool> active{true};
    std::atomic<bool> closed{};
    std::mutex operation_mutex;
    std::mutex wait_mutex;
    std::condition_variable_any wake;
    std::jthread worker;

    LeaseSession(
        std::shared_ptr<KuraMetadataBackend> backend_value,
        const LeaseId id_value,
        const FencingToken token_value,
        const std::chrono::milliseconds interval)
        : backend(std::move(backend_value)),
          id(id_value),
          token(token_value),
          keepalive_interval(interval) {}

    void start() {
        worker = std::jthread([this](const std::stop_token stop) {
            std::unique_lock wait_lock(wait_mutex);
            while (!stop.stop_requested()) {
                static_cast<void>(wake.wait_for(
                    wait_lock,
                    stop,
                    keepalive_interval,
                    [] { return false; }));
                if (stop.stop_requested()) {
                    return;
                }
                wait_lock.unlock();
                static_cast<void>(keep_alive());
                wait_lock.lock();
                if (!active.load()) {
                    return;
                }
            }
        });
    }

    [[nodiscard]] bool keep_alive() {
        std::scoped_lock lock(operation_mutex);
        if (!active.load()) {
            return false;
        }
        try {
            const auto result = backend->keep_alive(id, token);
            if (result.code != LeaseResultCode::ok) {
                active.store(false);
            }
        } catch (...) {
            active.store(false);
        }
        return active.load();
    }

    void close() noexcept {
        if (worker.joinable()) {
            worker.request_stop();
            wake.notify_all();
            worker.join();
        }
        std::scoped_lock lock(operation_mutex);
        if (closed.exchange(true)) {
            return;
        }
        active.store(false);
        try {
            static_cast<void>(backend->revoke_lease(id, token));
        } catch (...) {
            // Destructors cannot report cleanup failures. Lease expiry remains
            // the final cleanup mechanism.
        }
    }

    ~LeaseSession() {
        close();
    }
};

bool uncertain_status(const StatusCode status) {
    return status == StatusCode::not_leader
        || status == StatusCode::no_quorum
        || status == StatusCode::deadline_exceeded;
}

}  // namespace

struct InProcessKuraMetadataBackend::Impl {
    std::shared_ptr<InMemoryMetadataStore> store;
    std::shared_ptr<TickSource> ticks;
    mutable std::mutex mutex;
    std::uint64_t request_sequence{1};

    Impl(
        std::shared_ptr<InMemoryMetadataStore> store_value,
        std::shared_ptr<TickSource> ticks_value)
        : store(std::move(store_value)),
          ticks(std::move(ticks_value)) {}
};

InProcessKuraMetadataBackend::InProcessKuraMetadataBackend(
    std::shared_ptr<InMemoryMetadataStore> store)
    : InProcessKuraMetadataBackend(
          std::move(store),
          std::make_shared<SteadyTickSource>()) {}

InProcessKuraMetadataBackend::InProcessKuraMetadataBackend(
    std::shared_ptr<InMemoryMetadataStore> store,
    std::shared_ptr<TickSource> ticks)
    : impl_(std::make_unique<Impl>(
          std::move(store),
          std::move(ticks))) {
    if (!impl_->store || !impl_->ticks) {
        throw std::invalid_argument(
            "in-process backend requires a store and tick source");
    }
}

InProcessKuraMetadataBackend::~InProcessKuraMetadataBackend() = default;

LeaseTick InProcessKuraMetadataBackend::current_tick() const {
    return impl_->ticks->current_tick();
}

StoreRead InProcessKuraMetadataBackend::get(const ByteSequence& key) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->get(key);
}

RangeRead InProcessKuraMetadataBackend::range(
    const ByteSequence& start,
    const ByteSequence& end) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->range(start, end);
}

TransactionResult InProcessKuraMetadataBackend::transaction(
    TransactionRequest request) {
    std::scoped_lock lock(impl_->mutex);
    request.lease_tick = impl_->ticks->current_tick();
    return impl_->store->transaction(request);
}

LeaseGrantResult InProcessKuraMetadataBackend::grant_lease(
    const LeaseDuration ttl) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->grant_lease(LeaseGrantRequest{
        .request_id = RequestId{0, impl_->request_sequence++},
        .ttl = ttl,
        .tick = impl_->ticks->current_tick()});
}

LeaseLookupResult InProcessKuraMetadataBackend::keep_alive(
    const LeaseId id,
    const FencingToken token) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->keep_alive(LeaseKeepAliveRequest{
        .id = id,
        .fencing_token = token,
        .tick = impl_->ticks->current_tick()});
}

LeaseCleanupResult InProcessKuraMetadataBackend::revoke_lease(
    const LeaseId id,
    const FencingToken token) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->revoke_lease(LeaseRevokeRequest{
        .request_id = RequestId{0, impl_->request_sequence++},
        .id = id,
        .fencing_token = token,
        .tick = impl_->ticks->current_tick()});
}

WatchResponse InProcessKuraMetadataBackend::create_watch(
    const WatchRequest& request) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->create_watch(request);
}

std::optional<WatchResponse> InProcessKuraMetadataBackend::poll_watch(
    const WatchId id) {
    std::scoped_lock lock(impl_->mutex);
    return impl_->store->poll_watch(id);
}

void InProcessKuraMetadataBackend::cancel_watch(const WatchId id) noexcept {
    try {
        std::scoped_lock lock(impl_->mutex);
        static_cast<void>(impl_->store->cancel_watch(id));
    } catch (...) {
    }
}

bool InProcessKuraMetadataBackend::collect_snapshot_if_unreferenced(
    const ByteSequence& current_key,
    const ByteSequence& snapshot_key,
    const ByteSequence& readers_start,
    const ByteSequence& readers_end,
    const std::string_view snapshot_id) {
    std::scoped_lock lock(impl_->mutex);
    const auto current = impl_->store->get(current_key);
    if (current.value
        && decode_pointer(current.value->value).snapshot_id == snapshot_id) {
        return false;
    }
    for (const auto& reader :
         impl_->store->range(readers_start, readers_end).values) {
        if (reader.value.to_string() == snapshot_id) {
            return false;
        }
    }
    const auto snapshot = impl_->store->get(snapshot_key);
    if (!snapshot.value) {
        return true;
    }
    const auto result = impl_->store->transaction(TransactionRequest{
        .request_id = RequestId{0, impl_->request_sequence++},
        .comparisons = {numeric_compare(
            snapshot_key,
            CompareTarget::mod_revision,
            CompareResult::equal,
            snapshot.value->mod_revision)},
        .lease_tick = impl_->ticks->current_tick(),
        .success = {DeleteRequest{
            .range = KeyRange{
                .start = snapshot_key,
                .end = exact_key_end(snapshot_key)}}}});
    return result.succeeded;
}

struct WriterGuard::Impl {
    std::shared_ptr<LeaseSession> lease;
    std::string table_prefix;
    Revision observed_revision;
};

WriterGuard::WriterGuard(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

WriterGuard::WriterGuard(WriterGuard&&) noexcept = default;
WriterGuard& WriterGuard::operator=(WriterGuard&&) noexcept = default;
WriterGuard::~WriterGuard() = default;

LeaseId WriterGuard::lease() const noexcept {
    return impl_ ? impl_->lease->id : LeaseId{};
}

FencingToken WriterGuard::fencing_token() const noexcept {
    return impl_ ? impl_->lease->token : FencingToken{};
}

Revision WriterGuard::observed_revision() const noexcept {
    return impl_ ? impl_->observed_revision : Revision{};
}

bool WriterGuard::active() const noexcept {
    return impl_ && impl_->lease->active.load();
}

bool WriterGuard::keep_alive() {
    return impl_ && impl_->lease->keep_alive();
}

void WriterGuard::close() noexcept {
    if (impl_) {
        impl_->lease->close();
    }
}

struct ReaderGuard::Impl {
    std::shared_ptr<LeaseSession> lease;
    std::string reader_id;
    SnapshotPointer snapshot;
};

ReaderGuard::ReaderGuard(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReaderGuard::ReaderGuard(ReaderGuard&&) noexcept = default;
ReaderGuard& ReaderGuard::operator=(ReaderGuard&&) noexcept = default;
ReaderGuard::~ReaderGuard() = default;

std::string_view ReaderGuard::reader_id() const noexcept {
    return impl_ ? std::string_view{impl_->reader_id} : std::string_view{};
}

LeaseId ReaderGuard::lease() const noexcept {
    return impl_ ? impl_->lease->id : LeaseId{};
}

const SnapshotPointer& ReaderGuard::snapshot() const noexcept {
    static const SnapshotPointer empty;
    return impl_ ? impl_->snapshot : empty;
}

bool ReaderGuard::active() const noexcept {
    return impl_ && impl_->lease->active.load();
}

bool ReaderGuard::keep_alive() {
    return impl_ && impl_->lease->keep_alive();
}

void ReaderGuard::close() noexcept {
    if (impl_) {
        impl_->lease->close();
    }
}

KuraClientError::KuraClientError(
    const StatusCode code,
    std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

StatusCode KuraClientError::code() const noexcept {
    return code_;
}

KuraClient::KuraClient(
    std::shared_ptr<KuraMetadataBackend> backend,
    std::string catalog_id)
    : backend_(std::move(backend)),
      catalog_id_(std::move(catalog_id)),
      client_id_(next_client_id.fetch_add(1)) {
    if (!backend_) {
        throw std::invalid_argument("Kura client requires a backend");
    }
    validate_identifier(catalog_id_, "catalog ID");
}

std::string KuraClient::table_prefix(const std::string_view table_id) const {
    validate_identifier(table_id, "table ID");
    return "/kura/v1/catalogs/" + catalog_id_ + "/tables/"
        + std::string(table_id) + "/";
}

RequestId KuraClient::next_request_id() {
    return RequestId{client_id_, next_sequence_++};
}

WriterGuard KuraClient::acquire_writer(
    const std::string_view table_id,
    const std::chrono::seconds ttl) {
    if (ttl <= std::chrono::seconds::zero()) {
        throw std::invalid_argument("writer TTL must be positive");
    }
    const auto prefix = table_prefix(table_id);
    const auto current = backend_->get(bytes(prefix + "current"));
    const auto granted = backend_->grant_lease(
        LeaseDuration{static_cast<std::uint64_t>(ttl.count())});
    const auto writer_key = bytes(prefix + "writer");
    const auto writer_value = bytes(
        std::to_string(granted.lease.id.value) + ":"
        + std::to_string(granted.lease.fencing_token.value));
    const auto claim = backend_->transaction(TransactionRequest{
        .request_id = next_request_id(),
        .comparisons = {numeric_compare(
            writer_key,
            CompareTarget::version,
            CompareResult::equal,
            0)},
        .lease_ownership = {LeaseOwnership{
            granted.lease.id,
            granted.lease.fencing_token}},
        .success = {PutRequest{
            .key = writer_key,
            .value = writer_value,
            .lease_id = granted.lease.id.value}}});
    if (!claim.succeeded) {
        static_cast<void>(backend_->revoke_lease(
            granted.lease.id,
            granted.lease.fencing_token));
        throw KuraClientError(
            StatusCode::comparison_failed,
            "another writer owns the table");
    }

    auto lease = std::make_shared<LeaseSession>(
        backend_,
        granted.lease.id,
        granted.lease.fencing_token,
        std::max(
            std::chrono::milliseconds{1},
            std::chrono::duration_cast<std::chrono::milliseconds>(ttl)
                / 3));
    lease->start();
    return WriterGuard{std::make_unique<WriterGuard::Impl>(
        WriterGuard::Impl{
            .lease = std::move(lease),
            .table_prefix = prefix,
            .observed_revision = Revision{
                current.value ? current.value->mod_revision : 0}})};
}

PublishResult KuraClient::publish_snapshot(
    WriterGuard& guard,
    const Revision expected_revision,
    const SnapshotPointer& pointer) {
    if (!guard.impl_ || !guard.active()) {
        throw KuraClientError(
            StatusCode::lease_not_found,
            "writer guard is no longer active");
    }
    const auto encoded = encode_pointer(pointer);
    const auto current_key = bytes(guard.impl_->table_prefix + "current");
    const auto writer_key = bytes(guard.impl_->table_prefix + "writer");
    const auto snapshot_key = bytes(
        guard.impl_->table_prefix + "snapshots/" + pointer.snapshot_id);
    const auto request = TransactionRequest{
        .request_id = next_request_id(),
        .comparisons = {
            numeric_compare(
                current_key,
                CompareTarget::mod_revision,
                CompareResult::equal,
                expected_revision.value),
            numeric_compare(
                writer_key,
                CompareTarget::lease_id,
                CompareResult::equal,
                guard.lease().value),
        },
        .lease_ownership = {LeaseOwnership{
            guard.lease(),
            guard.fencing_token()}},
        .success = {
            PutRequest{.key = current_key, .value = encoded},
            PutRequest{.key = snapshot_key, .value = encoded},
        }};

    try {
        const auto result = backend_->transaction(request);
        if (!result.succeeded) {
            const auto current = backend_->get(current_key);
            return {
                .status = PublishStatus::conflict,
                .revision = Revision{
                    current.value ? current.value->mod_revision : 0},
                .current = current.value
                    ? std::optional{decode_pointer(current.value->value)}
                    : std::nullopt};
        }
        return {
            .status = PublishStatus::published,
            .revision = Revision{result.header.revision},
            .current = pointer};
    } catch (const KuraClientError& error) {
        if (!uncertain_status(error.code())) {
            throw;
        }
        const auto current = backend_->get(current_key);
        if (current.value && current.value->value == encoded) {
            return {
                .status =
                    PublishStatus::recovered_after_uncertain_response,
                .revision = Revision{current.value->mod_revision},
                .current = pointer};
        }
        throw;
    }
}

ReaderGuard KuraClient::register_reader(
    const std::string_view table_id,
    const SnapshotPointer& snapshot,
    const std::chrono::seconds ttl) {
    if (ttl <= std::chrono::seconds::zero()) {
        throw std::invalid_argument("reader TTL must be positive");
    }
    const auto encoded = encode_pointer(snapshot);
    const auto prefix = table_prefix(table_id);
    const auto granted = backend_->grant_lease(
        LeaseDuration{static_cast<std::uint64_t>(ttl.count())});
    const auto reader_id = std::to_string(client_id_) + "-"
        + std::to_string(next_sequence_++);
    const auto reader_key = bytes(prefix + "readers/" + reader_id);
    const auto snapshot_key =
        bytes(prefix + "snapshots/" + snapshot.snapshot_id);
    const auto registration = backend_->transaction(TransactionRequest{
        .request_id = next_request_id(),
        .comparisons = {Compare{
            .key = snapshot_key,
            .target = CompareTarget::value,
            .result = CompareResult::equal,
            .expected = encoded}},
        .lease_ownership = {LeaseOwnership{
            granted.lease.id,
            granted.lease.fencing_token}},
        .success = {PutRequest{
            .key = reader_key,
            .value = bytes(snapshot.snapshot_id),
            .lease_id = granted.lease.id.value}}});
    if (!registration.succeeded) {
        static_cast<void>(backend_->revoke_lease(
            granted.lease.id,
            granted.lease.fencing_token));
        throw KuraClientError(
            StatusCode::comparison_failed,
            "snapshot is not registered");
    }

    auto lease = std::make_shared<LeaseSession>(
        backend_,
        granted.lease.id,
        granted.lease.fencing_token,
        std::max(
            std::chrono::milliseconds{1},
            std::chrono::duration_cast<std::chrono::milliseconds>(ttl)
                / 3));
    lease->start();
    return ReaderGuard{std::make_unique<ReaderGuard::Impl>(
        ReaderGuard::Impl{
            .lease = std::move(lease),
            .reader_id = reader_id,
            .snapshot = snapshot})};
}

std::optional<SnapshotUpdate> KuraClient::await_snapshot_change(
    const std::string_view table_id,
    const Revision from_revision,
    const std::chrono::milliseconds timeout) {
    if (from_revision.value < 0
        || from_revision.value == std::numeric_limits<std::int64_t>::max()
        || timeout < std::chrono::milliseconds::zero()) {
        throw std::invalid_argument(
            "watch revision and timeout must not be negative");
    }
    const auto current_key = bytes(table_prefix(table_id) + "current");
    const WatchId watch_id{next_watch_id_++};
    try {
        static_cast<void>(backend_->create_watch(WatchRequest{
            .id = watch_id,
            .range = KeyRange{.start = current_key},
            .start_revision = Revision{from_revision.value + 1}}));
    } catch (const StoreError& error) {
        if (error.code() != StatusCode::compacted) {
            throw;
        }
        auto current = current_snapshot(table_id);
        if (current) {
            current->full_resynchronization = true;
        }
        return current;
    } catch (const KuraClientError& error) {
        if (error.code() != StatusCode::compacted) {
            throw;
        }
        auto current = current_snapshot(table_id);
        if (current) {
            current->full_resynchronization = true;
        }
        return current;
    }

    struct WatchCleanup {
        KuraMetadataBackend& backend;
        WatchId id;
        ~WatchCleanup() {
            backend.cancel_watch(id);
        }
    } cleanup{*backend_, watch_id};

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        const auto response = backend_->poll_watch(watch_id);
        if (response) {
            if (response->status == StatusCode::compacted) {
                auto current = current_snapshot(table_id);
                if (current) {
                    current->full_resynchronization = true;
                }
                return current;
            }
            for (const auto& event : response->events) {
                if (event.mutation.current) {
                    return SnapshotUpdate{
                        .pointer =
                            decode_pointer(event.mutation.current->value),
                        .revision = event.revision};
                }
            }
        }
        if (timeout == std::chrono::milliseconds::zero()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < deadline);
    return std::nullopt;
}

bool KuraClient::collect_snapshot(
    const std::string_view table_id,
    const std::string_view snapshot_id) {
    validate_identifier(snapshot_id, "snapshot ID");
    const auto prefix = table_prefix(table_id);
    const auto reader_prefix = bytes(prefix + "readers/");
    const auto reader_end = prefix_range_end(reader_prefix);
    if (!reader_end) {
        throw std::logic_error("reader namespace has no finite range");
    }
    return backend_->collect_snapshot_if_unreferenced(
        bytes(prefix + "current"),
        bytes(prefix + "snapshots/" + std::string(snapshot_id)),
        reader_prefix,
        *reader_end,
        snapshot_id);
}

std::optional<SnapshotUpdate> KuraClient::current_snapshot(
    const std::string_view table_id) {
    const auto current =
        backend_->get(bytes(table_prefix(table_id) + "current"));
    if (!current.value) {
        return std::nullopt;
    }
    return SnapshotUpdate{
        .pointer = decode_pointer(current.value->value),
        .revision = Revision{current.revision}};
}

}  // namespace kura::metadata
