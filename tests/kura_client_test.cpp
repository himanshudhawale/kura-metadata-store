#include "kura/metadata/in_memory_metadata_store.hpp"
#include "kura/metadata/kura/kura_client.hpp"

#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

using namespace std::chrono_literals;
using kura::metadata::ByteSequence;
using kura::metadata::FencingToken;
using kura::metadata::InMemoryMetadataStore;
using kura::metadata::InProcessKuraMetadataBackend;
using kura::metadata::KuraClient;
using kura::metadata::KuraClientError;
using kura::metadata::KuraMetadataBackend;
using kura::metadata::LeaseCleanupResult;
using kura::metadata::LeaseDuration;
using kura::metadata::LeaseGrantResult;
using kura::metadata::LeaseId;
using kura::metadata::LeaseLookupResult;
using kura::metadata::LeaseTick;
using kura::metadata::PublishStatus;
using kura::metadata::RangeRead;
using kura::metadata::Revision;
using kura::metadata::SnapshotPointer;
using kura::metadata::StatusCode;
using kura::metadata::StoreLimits;
using kura::metadata::StoreRead;
using kura::metadata::TransactionRequest;
using kura::metadata::TransactionResult;
using kura::metadata::WatchId;
using kura::metadata::WatchRequest;
using kura::metadata::WatchResponse;

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

SnapshotPointer pointer(const std::string& id) {
    return {
        .snapshot_id = id,
        .manifest_uri = "object://bucket/" + id,
        .schema_id = "schema-1",
        .integrity_hash = {0x01, 0x02, 0x03, 0x04}};
}

class ManualTicks final
    : public InProcessKuraMetadataBackend::TickSource {
public:
    [[nodiscard]] LeaseTick current_tick() const override {
        return LeaseTick{value_.load()};
    }

    void set(const std::uint64_t value) {
        value_.store(value);
    }

private:
    std::atomic<std::uint64_t> value_{};
};

class UncertainCommitBackend final : public KuraMetadataBackend {
public:
    explicit UncertainCommitBackend(
        std::shared_ptr<KuraMetadataBackend> delegate)
        : delegate_(std::move(delegate)) {}

    void fail_next_transaction_after_commit() {
        fail_after_commit_.store(true);
    }

    [[nodiscard]] bool wait_for_keepalive(
        const std::chrono::milliseconds timeout) {
        std::unique_lock lock(keepalive_mutex_);
        return keepalive_ready_.wait_for(
            lock,
            timeout,
            [&] { return keepalive_count_ != 0; });
    }

    [[nodiscard]] LeaseTick current_tick() const override {
        return delegate_->current_tick();
    }

    [[nodiscard]] StoreRead get(const ByteSequence& key) override {
        return delegate_->get(key);
    }

    [[nodiscard]] RangeRead range(
        const ByteSequence& start,
        const ByteSequence& end) override {
        return delegate_->range(start, end);
    }

    [[nodiscard]] TransactionResult transaction(
        TransactionRequest request) override {
        auto result = delegate_->transaction(std::move(request));
        if (fail_after_commit_.exchange(false)) {
            throw KuraClientError(
                StatusCode::not_leader,
                "injected response loss after commit");
        }
        return result;
    }

    [[nodiscard]] LeaseGrantResult grant_lease(
        const LeaseDuration ttl) override {
        return delegate_->grant_lease(ttl);
    }

    [[nodiscard]] LeaseLookupResult keep_alive(
        const LeaseId id,
        const FencingToken token) override {
        auto result = delegate_->keep_alive(id, token);
        {
            std::scoped_lock lock(keepalive_mutex_);
            ++keepalive_count_;
        }
        keepalive_ready_.notify_all();
        return result;
    }

    [[nodiscard]] LeaseCleanupResult revoke_lease(
        const LeaseId id,
        const FencingToken token) override {
        return delegate_->revoke_lease(id, token);
    }

    [[nodiscard]] WatchResponse create_watch(
        const WatchRequest& request) override {
        return delegate_->create_watch(request);
    }

    [[nodiscard]] std::optional<WatchResponse> poll_watch(
        const WatchId id) override {
        return delegate_->poll_watch(id);
    }

    void cancel_watch(const WatchId id) noexcept override {
        delegate_->cancel_watch(id);
    }

    [[nodiscard]] bool collect_snapshot_if_unreferenced(
        const ByteSequence& current_key,
        const ByteSequence& snapshot_key,
        const ByteSequence& readers_start,
        const ByteSequence& readers_end,
        const std::string_view snapshot_id) override {
        return delegate_->collect_snapshot_if_unreferenced(
            current_key,
            snapshot_key,
            readers_start,
            readers_end,
            snapshot_id);
    }

private:
    std::shared_ptr<KuraMetadataBackend> delegate_;
    std::atomic<bool> fail_after_commit_{};
    std::mutex keepalive_mutex_;
    std::condition_variable keepalive_ready_;
    std::size_t keepalive_count_{};
};

void two_writer_contenders_have_one_winner() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient first(backend);
    KuraClient second(backend);
    std::barrier start{3};
    std::barrier acquired{3};
    std::atomic<int> owners{};
    std::atomic<int> published{};

    auto contender = [&](KuraClient& client, const SnapshotPointer& snapshot) {
        start.arrive_and_wait();
        try {
            auto writer = client.acquire_writer("orders", 30s);
            ++owners;
            acquired.arrive_and_wait();
            if (client.publish_snapshot(
                          writer,
                          writer.observed_revision(),
                          snapshot)
                    .published()) {
                ++published;
            }
        } catch (const KuraClientError& error) {
            expect(
                error.code() == StatusCode::comparison_failed,
                "losing writer reports an ownership conflict");
            acquired.arrive_and_wait();
        }
    };

    std::jthread first_thread(
        contender,
        std::ref(first),
        pointer("snapshot-a"));
    std::jthread second_thread(
        contender,
        std::ref(second),
        pointer("snapshot-b"));
    start.arrive_and_wait();
    acquired.arrive_and_wait();
    first_thread.join();
    second_thread.join();

    expect(owners.load() == 1, "exactly one writer acquires ownership");
    expect(published.load() == 1, "exactly one contender publishes");
    expect(
        store->get(bytes(
            "/kura/v1/catalogs/default/tables/orders/current"))
                .value
                ->version
            == 1,
        "the current pointer is created once");
}

void paused_writer_is_fenced_after_lease_loss() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient client(backend);
    auto writer = client.acquire_writer("orders", 2s);

    const auto expired = store->expire_leases(LeaseTick{2});
    expect(
        expired.leases == std::vector<LeaseId>{writer.lease()},
        "the paused writer lease expires");
    ticks->set(2);
    expect(!writer.keep_alive(), "expired generation cannot be revived");
    expect_throws<KuraClientError>(
        [&] {
            static_cast<void>(client.publish_snapshot(
                writer,
                Revision{0},
                pointer("stale")));
        },
        "an inactive guard cannot publish");
    expect(
        !client.current_snapshot("orders"),
        "lease loss leaves no current pointer");
}

void live_reader_registration_prevents_collection() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient client(backend);
    const auto snapshot = pointer("snapshot-1");
    auto writer = client.acquire_writer("orders", 30s);
    const auto publication =
        client.publish_snapshot(writer, Revision{0}, snapshot);
    expect(publication.published(), "fixture snapshot publishes");

    auto reader = client.register_reader("orders", snapshot, 30s);
    const auto replacement = client.publish_snapshot(
        writer,
        publication.revision,
        pointer("snapshot-2"));
    expect(replacement.published(), "fixture advances the current snapshot");
    expect(reader.active(), "reader registration owns a live lease");
    expect(
        !client.collect_snapshot("orders", snapshot.snapshot_id),
        "live reader registration blocks snapshot collection");
    reader.close();
    expect(
        client.collect_snapshot("orders", snapshot.snapshot_id),
        "snapshot metadata is collectible after reader cleanup");
}

void compacted_watch_resynchronizes_from_current() {
    StoreLimits limits;
    limits.max_watch_history_revisions = 1;
    auto store = std::make_shared<InMemoryMetadataStore>(limits);
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient client(backend);
    auto writer = client.acquire_writer("orders", 30s);

    const auto first = client.publish_snapshot(
        writer,
        Revision{0},
        pointer("snapshot-1"));
    const auto second = client.publish_snapshot(
        writer,
        first.revision,
        pointer("snapshot-2"));
    const auto third = client.publish_snapshot(
        writer,
        second.revision,
        pointer("snapshot-3"));
    expect(third.published(), "latest snapshot publishes");
    static_cast<void>(store->put(bytes("unrelated"), bytes("change")));

    const auto update = client.await_snapshot_change(
        "orders",
        first.revision,
        0ms);
    expect(update.has_value(), "compacted watch performs a current read");
    expect(
        update->full_resynchronization,
        "compaction is explicit to the caller");
    expect(
        update->pointer == pointer("snapshot-3")
            && update->revision == Revision{store->revision()}
            && update->revision.value > third.revision.value,
        "full resynchronization returns the full-read revision");
}

void retained_watch_delivers_the_changed_pointer() {
    StoreLimits limits;
    limits.max_watch_history_revisions = 16;
    auto store = std::make_shared<InMemoryMetadataStore>(limits);
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient client(backend);
    auto writer = client.acquire_writer("orders", 30s);
    const auto first = client.publish_snapshot(
        writer,
        Revision{0},
        pointer("snapshot-1"));
    const auto second = client.publish_snapshot(
        writer,
        first.revision,
        pointer("snapshot-2"));

    const auto update = client.await_snapshot_change(
        "orders",
        first.revision,
        0ms);
    expect(update.has_value(), "retained update is replayed");
    expect(
        !update->full_resynchronization
            && update->pointer == pointer("snapshot-2")
            && update->revision == second.revision,
        "retained watch preserves the event revision");
}

void uncertain_committed_publication_is_resolved_without_duplicate() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto in_process = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    auto uncertain =
        std::make_shared<UncertainCommitBackend>(in_process);
    KuraClient client(uncertain);
    auto writer = client.acquire_writer("orders", 30s);
    const auto snapshot = pointer("snapshot-1");

    uncertain->fail_next_transaction_after_commit();
    const auto recovered =
        client.publish_snapshot(writer, Revision{0}, snapshot);
    expect(
        recovered.status
            == PublishStatus::recovered_after_uncertain_response,
        "client resolves a lost success response by reading current");
    const auto retry =
        client.publish_snapshot(writer, Revision{0}, snapshot);
    expect(
        retry.status == PublishStatus::conflict,
        "retry with the consumed base revision cannot mutate again");
    const auto stored = store->get(bytes(
        "/kura/v1/catalogs/default/tables/orders/current"));
    expect(
        stored.value && stored.value->version == 1,
        "leader-failure fake produces no duplicate current update");
}

void guards_cleanup_and_validate_snapshot_lifecycle() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto backend = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    KuraClient client(backend);

    expect_throws<KuraClientError>(
        [&] {
            static_cast<void>(client.register_reader(
                "orders",
                pointer("missing"),
                10s));
        },
        "reader cannot protect an unregistered snapshot");

    {
        auto writer = client.acquire_writer("orders", 10s);
        expect(writer.keep_alive(), "guard owns successful keepalive");
    }
    auto replacement = client.acquire_writer("orders", 10s);
    expect(
        replacement.active(),
        "writer guard destruction revokes ownership and attached key");
}

void guards_run_automatic_keepalive() {
    auto store = std::make_shared<InMemoryMetadataStore>();
    auto ticks = std::make_shared<ManualTicks>();
    auto in_process = std::make_shared<InProcessKuraMetadataBackend>(
        store,
        ticks);
    auto observed = std::make_shared<UncertainCommitBackend>(in_process);
    KuraClient client(observed);
    auto writer = client.acquire_writer("orders", 1s);

    expect(
        observed->wait_for_keepalive(2s),
        "writer guard periodically invokes keepalive without caller action");
    expect(writer.active(), "successful automatic keepalive keeps guard active");
}

}  // namespace

int main() {
    try {
        two_writer_contenders_have_one_winner();
        paused_writer_is_fenced_after_lease_loss();
        live_reader_registration_prevents_collection();
        compacted_watch_resynchronizes_from_current();
        retained_watch_delivers_the_changed_pointer();
        uncertain_committed_publication_is_resolved_without_duplicate();
        guards_cleanup_and_validate_snapshot_lifecycle();
        guards_run_automatic_keepalive();
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return 1;
    }
    std::cout << "all Kura client tests passed\n";
    return 0;
}
