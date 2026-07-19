#pragma once

namespace kura::metadata {

enum class StatusCode {
    ok,
    invalid_argument,
    comparison_failed,
    compacted,
    future_revision,
    lease_not_found,
    not_leader,
    no_quorum,
    deadline_exceeded,
    permission_denied,
    quota_exceeded,
    corruption
};

}  // namespace kura::metadata
