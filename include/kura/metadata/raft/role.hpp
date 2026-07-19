#pragma once

namespace kura::metadata {

enum class RaftRole {
    follower,
    candidate,
    leader,
    learner
};

}  // namespace kura::metadata
