#pragma once

#include "kura/metadata/kura/snapshot_pointer.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <memory>
#include <string_view>

namespace kura::metadata {

class KuraClient;

class ReaderGuard {
public:
    ReaderGuard(const ReaderGuard&) = delete;
    ReaderGuard& operator=(const ReaderGuard&) = delete;
    ReaderGuard(ReaderGuard&&) noexcept;
    ReaderGuard& operator=(ReaderGuard&&) noexcept;
    ~ReaderGuard();

    [[nodiscard]] std::string_view reader_id() const noexcept;
    [[nodiscard]] LeaseId lease() const noexcept;
    [[nodiscard]] const SnapshotPointer& snapshot() const noexcept;
    [[nodiscard]] bool active() const noexcept;

    [[nodiscard]] bool keep_alive();
    void close() noexcept;

private:
    struct Impl;

    explicit ReaderGuard(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;

    friend class KuraClient;
};

}  // namespace kura::metadata
