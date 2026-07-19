#pragma once

#include "kura/metadata/core/response_header.hpp"
#include "kura/metadata/core/status_code.hpp"
#include "kura/metadata/watch/watch_event.hpp"
#include "kura/metadata/watch/watch_id.hpp"

#include <vector>

namespace kura::metadata {

struct WatchResponse {
    ResponseHeader header;
    WatchId id;
    std::vector<WatchEvent> events;
    std::int64_t compact_revision{};
    StatusCode status{StatusCode::ok};
    bool cancelled{};
};

}  // namespace kura::metadata
