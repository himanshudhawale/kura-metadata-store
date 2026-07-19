#pragma once

#include "kura/metadata/watch/watch_request.hpp"
#include "kura/metadata/watch/watch_response.hpp"

namespace kura::metadata {

class WatchStream {
public:
    virtual ~WatchStream() = default;

    virtual void create(const WatchRequest& request) = 0;
    virtual void cancel(WatchId id) = 0;
    [[nodiscard]] virtual WatchResponse next() = 0;
};

}  // namespace kura::metadata
