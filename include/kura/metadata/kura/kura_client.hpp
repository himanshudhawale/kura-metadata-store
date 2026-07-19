#pragma once

#include "kura/metadata/kura/reader_guard.hpp"
#include "kura/metadata/kura/writer_guard.hpp"

#include <chrono>
#include <string_view>

namespace kura::metadata {

class KuraClient {
public:
    virtual ~KuraClient() = default;

    [[nodiscard]] virtual WriterGuard acquire_writer(
        std::string_view table_id,
        std::chrono::seconds ttl) = 0;
    [[nodiscard]] virtual ReaderGuard register_reader(
        std::string_view table_id,
        std::chrono::seconds ttl) = 0;
};

}  // namespace kura::metadata
