#pragma once

#include "kura/metadata/core/status_code.hpp"

#include <stdexcept>
#include <string>

namespace kura::metadata {

class StoreError : public std::runtime_error {
public:
    StoreError(StatusCode code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {}

    [[nodiscard]] StatusCode code() const noexcept {
        return code_;
    }

private:
    StatusCode code_;
};

}  // namespace kura::metadata
