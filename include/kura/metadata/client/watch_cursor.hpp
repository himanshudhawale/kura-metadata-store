#pragma once

#include "kura/metadata/core/revision.hpp"

namespace kura::metadata {

struct WatchCursor {
    Revision last_delivered;
    Revision compacted_through;
};

}  // namespace kura::metadata
