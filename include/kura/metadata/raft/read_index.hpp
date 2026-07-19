#pragma once

#include "kura/metadata/core/request_id.hpp"
#include "kura/metadata/raft/log_index.hpp"

namespace kura::metadata {

struct ReadIndexRequest {
    RequestId request_id;
};

struct ReadIndexResponse {
    RequestId request_id;
    LogIndex committed_index;
};

}  // namespace kura::metadata
