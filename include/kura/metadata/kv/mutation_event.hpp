#pragma once

#include "kura/metadata/metadata_store.hpp"

#include <optional>

namespace kura::metadata {

enum class MutationEventType {
    put,
    erase
};

struct MutationEvent {
    MutationEventType type;
    KeyValue current;
    std::optional<KeyValue> previous;
};

}  // namespace kura::metadata
