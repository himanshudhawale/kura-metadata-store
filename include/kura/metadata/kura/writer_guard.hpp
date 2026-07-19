#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/lease/lease_id.hpp"

namespace kura::metadata {

struct WriterGuard {
    LeaseId lease;
    Revision fencing_revision;
};

}  // namespace kura::metadata
