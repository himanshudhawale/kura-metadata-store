#pragma once

#include "kura/metadata/core/status_code.hpp"
#include "kura/metadata/kura/kura_metadata_backend.hpp"
#include "kura/metadata/kura/reader_guard.hpp"
#include "kura/metadata/kura/writer_guard.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kura::metadata {

enum class PublishStatus {
    published,
    conflict,
    recovered_after_uncertain_response
};

struct PublishResult {
    PublishStatus status{PublishStatus::conflict};
    Revision revision;
    std::optional<SnapshotPointer> current;

    [[nodiscard]] bool published() const noexcept {
        return status != PublishStatus::conflict;
    }
};

struct SnapshotUpdate {
    SnapshotPointer pointer;
    Revision revision;
    bool full_resynchronization{};
};

class KuraClientError : public std::runtime_error {
public:
    KuraClientError(StatusCode code, std::string message);

    [[nodiscard]] StatusCode code() const noexcept;

private:
    StatusCode code_;
};

class KuraClient {
public:
    explicit KuraClient(
        std::shared_ptr<KuraMetadataBackend> backend,
        std::string catalog_id = "default");

    [[nodiscard]] WriterGuard acquire_writer(
        std::string_view table_id,
        std::chrono::seconds ttl);

    [[nodiscard]] PublishResult publish_snapshot(
        WriterGuard& guard,
        Revision expected_revision,
        const SnapshotPointer& pointer);

    [[nodiscard]] ReaderGuard register_reader(
        std::string_view table_id,
        const SnapshotPointer& snapshot,
        std::chrono::seconds ttl);

    [[nodiscard]] std::optional<SnapshotUpdate> await_snapshot_change(
        std::string_view table_id,
        Revision from_revision,
        std::chrono::milliseconds timeout = std::chrono::seconds{30});

    [[nodiscard]] bool collect_snapshot(
        std::string_view table_id,
        std::string_view snapshot_id);

    [[nodiscard]] std::optional<SnapshotUpdate> current_snapshot(
        std::string_view table_id);

private:
    [[nodiscard]] std::string table_prefix(std::string_view table_id) const;
    [[nodiscard]] RequestId next_request_id();

    std::shared_ptr<KuraMetadataBackend> backend_;
    std::string catalog_id_;
    std::uint64_t client_id_;
    std::uint64_t next_sequence_{1};
    std::int64_t next_watch_id_{1};
};

}  // namespace kura::metadata
