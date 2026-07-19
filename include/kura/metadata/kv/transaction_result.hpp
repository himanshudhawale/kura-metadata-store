#pragma once

#include "kura/metadata/core/response_header.hpp"
#include "kura/metadata/kv/operation_result.hpp"

#include <cstddef>
#include <variant>
#include <vector>

namespace kura::metadata {

struct DeleteRangeResult {
    std::size_t deleted{};
    std::vector<KeyValue> previous;
    std::int64_t revision{};

    bool operator==(const DeleteRangeResult&) const = default;
};

using TransactionOperationResult =
    std::variant<RangeRead, PutResult, DeleteRangeResult>;

struct TransactionResult {
    ResponseHeader header;
    bool succeeded{};
    std::vector<TransactionOperationResult> responses;
};

}  // namespace kura::metadata
