#pragma once

#include "kura/metadata/byte_sequence.hpp"
#include "kura/metadata/core/request_id.hpp"

#include <cstdint>

namespace kura::metadata {

struct PutRequest {
    RequestId request_id;
    ByteSequence key;
    ByteSequence value;
    std::int64_t lease_id{};
    bool return_previous{};
};

}  // namespace kura::metadata
