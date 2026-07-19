#pragma once

#include <string>
#include <vector>

namespace kura::metadata {

struct AuthContext {
    std::string principal;
    std::vector<std::string> roles;
    bool peer{};
};

}  // namespace kura::metadata
