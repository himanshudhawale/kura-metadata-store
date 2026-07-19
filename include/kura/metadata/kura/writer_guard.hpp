#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <memory>

namespace kura::metadata {

class KuraClient;

class WriterGuard {
public:
    WriterGuard(const WriterGuard&) = delete;
    WriterGuard& operator=(const WriterGuard&) = delete;
    WriterGuard(WriterGuard&&) noexcept;
    WriterGuard& operator=(WriterGuard&&) noexcept;
    ~WriterGuard();

    [[nodiscard]] LeaseId lease() const noexcept;
    [[nodiscard]] FencingToken fencing_token() const noexcept;
    [[nodiscard]] Revision observed_revision() const noexcept;
    [[nodiscard]] bool active() const noexcept;

    [[nodiscard]] bool keep_alive();
    void close() noexcept;

private:
    struct Impl;

    explicit WriterGuard(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;

    friend class KuraClient;
};

}  // namespace kura::metadata
