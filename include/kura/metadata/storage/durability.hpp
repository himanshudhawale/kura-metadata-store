#pragma once

namespace kura::metadata {

enum class Durability {
    memory_only,
    flush,
    synchronize
};

}  // namespace kura::metadata
