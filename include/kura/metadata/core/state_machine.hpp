#pragma once

#include "kura/metadata/core/command.hpp"
#include "kura/metadata/core/command_result.hpp"

namespace kura::metadata {

class StateMachine {
public:
    virtual ~StateMachine() = default;

    [[nodiscard]] virtual CommandResult apply(
        const CommandEnvelope& command) = 0;
};

}  // namespace kura::metadata
