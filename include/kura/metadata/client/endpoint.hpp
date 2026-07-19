#pragma once

#include <string>

namespace kura::metadata {

struct Endpoint {
    std::string address;
    bool tls_required{true};
};

}  // namespace kura::metadata
