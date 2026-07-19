#pragma once

#include "kura/metadata/core/key_range.hpp"
#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/watch/watch_filter.hpp"
#include "kura/metadata/watch/watch_id.hpp"

namespace kura::metadata {

struct WatchRequest {
    WatchId id;
    KeyRange range;
    Revision start_revision;
    WatchFilter filter{WatchFilter::include_all};
    bool progress_notifications{};
};

}  // namespace kura::metadata
