#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/core/status_code.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct CommandResult {
    StatusCode status{StatusCode::ok};
    Revision revision;
    std::vector<std::uint8_t> payload;
};

}  // namespace kura::metadata
