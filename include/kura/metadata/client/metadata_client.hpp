#pragma once

#include "kura/metadata/kv/transaction_request.hpp"
#include "kura/metadata/kv/transaction_result.hpp"
#include "kura/metadata/metadata_store.hpp"

namespace kura::metadata {

class MetadataClient {
public:
    virtual ~MetadataClient() = default;

    [[nodiscard]] virtual StoreRead get(const ByteSequence& key) = 0;
    [[nodiscard]] virtual PutResult put(
        const ByteSequence& key,
        const ByteSequence& value) = 0;
    [[nodiscard]] virtual TransactionResult transaction(
        const TransactionRequest& request) = 0;
};

}  // namespace kura::metadata
