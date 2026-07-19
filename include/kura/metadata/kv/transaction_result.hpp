#pragma once

#include "kura/metadata/core/response_header.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct TransactionResult {
    ResponseHeader header;
    bool succeeded{};
    std::vector<std::vector<std::uint8_t>> responses;
};

}  // namespace kura::metadata
