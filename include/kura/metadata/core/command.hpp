#pragma once

#include "kura/metadata/core/request_id.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct CommandEnvelope {
    RequestId request_id;
    std::uint32_t type_tag{};
    std::vector<std::uint8_t> payload;

    bool operator==(const CommandEnvelope&) const = default;
};

}  // namespace kura::metadata
