#pragma once

#include "kura/metadata/byte_sequence.hpp"
#include "kura/metadata/kv/operation_result.hpp"
#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"

#include <cstdint>

namespace kura::metadata {

class MetadataStore {
public:
    virtual ~MetadataStore() = default;

    [[nodiscard]] virtual StoreRead get(const ByteSequence& key) const = 0;

    [[nodiscard]] virtual RangeRead range(
        const ByteSequence& start_inclusive,
        const ByteSequence& end_exclusive) const = 0;

    [[nodiscard]] virtual PutResult put(
        const ByteSequence& key,
        const ByteSequence& value) = 0;

    [[nodiscard]] virtual DeleteResult erase(const ByteSequence& key) = 0;

    [[nodiscard]] virtual CompareAndSetResult compare_and_set(
        const ByteSequence& key,
        std::int64_t expected_mod_revision,
        const ByteSequence& new_value) = 0;

    [[nodiscard]] virtual TransactionResult transaction(
        const TransactionRequest& request) = 0;

    [[nodiscard]] virtual std::int64_t revision() const = 0;
};

}  // namespace kura::metadata
