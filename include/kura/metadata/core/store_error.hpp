#pragma once

#include "kura/metadata/core/status_code.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>

namespace kura::metadata {

class StoreError : public std::runtime_error {
public:
    StoreError(
        StatusCode code,
        std::string message,
        std::int64_t compact_revision = 0)
        : std::runtime_error(std::move(message)),
          code_(code),
          compact_revision_(compact_revision) {}

    [[nodiscard]] StatusCode code() const noexcept {
        return code_;
    }

    [[nodiscard]] std::int64_t compact_revision() const noexcept {
        return compact_revision_;
    }

private:
    StatusCode code_;
    std::int64_t compact_revision_;
};

}  // namespace kura::metadata
