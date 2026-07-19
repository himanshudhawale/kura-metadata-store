#pragma once

#include "kura/metadata/core/key_range.hpp"
#include "kura/metadata/core/request_id.hpp"

namespace kura::metadata {

struct DeleteRequest {
    RequestId request_id;
    KeyRange range;
    bool return_previous{};
};

}  // namespace kura::metadata
