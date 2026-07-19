#pragma once

#include "kura/metadata/core/command.hpp"
#include "kura/metadata/raft/log_index.hpp"
#include "kura/metadata/raft/term.hpp"

namespace kura::metadata {

struct LogEntry {
    Term term;
    LogIndex index;
    CommandEnvelope command;
};

}  // namespace kura::metadata
