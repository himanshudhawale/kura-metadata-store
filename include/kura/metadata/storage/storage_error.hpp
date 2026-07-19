#pragma once

#include <stdexcept>

namespace kura::metadata {

class StorageError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}  // namespace kura::metadata
