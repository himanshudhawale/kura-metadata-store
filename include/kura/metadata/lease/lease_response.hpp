#pragma once

#include "kura/metadata/core/response_header.hpp"
#include "kura/metadata/lease/lease_record.hpp"

namespace kura::metadata {

struct LeaseResponse {
    ResponseHeader header;
    LeaseRecord lease;
};

}  // namespace kura::metadata
