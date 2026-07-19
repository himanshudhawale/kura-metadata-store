#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/kv/mutation_event.hpp"

namespace kura::metadata {

struct WatchEvent {
    Revision revision;
    MutationEvent mutation;
};

}  // namespace kura::metadata
