#pragma once

#include "kura/metadata/kv/delete_request.hpp"
#include "kura/metadata/kv/put_request.hpp"
#include "kura/metadata/kv/range_request.hpp"

#include <variant>

namespace kura::metadata {

using RequestOperation = std::variant<RangeRequest, PutRequest, DeleteRequest>;

}  // namespace kura::metadata
