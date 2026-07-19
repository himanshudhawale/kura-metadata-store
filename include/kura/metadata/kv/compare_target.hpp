#pragma once

namespace kura::metadata {

enum class CompareTarget {
    version,
    create_revision,
    mod_revision,
    value,
    lease_id
};

}  // namespace kura::metadata
