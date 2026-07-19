#pragma once

#include "kura/metadata/byte_sequence.hpp"
#include "kura/metadata/core/revision.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace kura::metadata::testing::linearizability {

using OperationId = std::uint64_t;
using ClientId = std::uint64_t;
using LogicalTime = std::uint64_t;

struct Get {
    ByteSequence key;

    bool operator==(const Get&) const = default;
};

struct Put {
    ByteSequence key;
    ByteSequence value;

    bool operator==(const Put&) const = default;
};

struct Erase {
    ByteSequence key;

    bool operator==(const Erase&) const = default;
};

struct CompareAndSwap {
    ByteSequence key;
    std::optional<ByteSequence> expected;
    std::optional<ByteSequence> desired;

    bool operator==(const CompareAndSwap&) const = default;
};

using Operation = std::variant<Get, Put, Erase, CompareAndSwap>;

struct VersionedValue {
    ByteSequence value;
    Revision modification_revision;

    bool operator==(const VersionedValue&) const = default;
};

struct GetResult {
    std::optional<VersionedValue> value;
    Revision revision;

    bool operator==(const GetResult&) const = default;
};

struct PutResult {
    Revision revision;

    bool operator==(const PutResult&) const = default;
};

struct EraseResult {
    bool erased{};
    Revision revision;

    bool operator==(const EraseResult&) const = default;
};

struct CompareAndSwapResult {
    bool exchanged{};
    Revision revision;

    bool operator==(const CompareAndSwapResult&) const = default;
};

using Result =
    std::variant<GetResult, PutResult, EraseResult, CompareAndSwapResult>;

enum class CompletionKind {
    succeeded,
    failed,
    timed_out,
    indeterminate
};

struct Invocation {
    OperationId operation_id{};
    ClientId client_id{};
    LogicalTime invoked_at{};
    Operation operation;

    bool operator==(const Invocation&) const = default;
};

struct Completion {
    OperationId operation_id{};
    LogicalTime completed_at{};
    CompletionKind kind{CompletionKind::succeeded};
    std::optional<Result> result;

    bool operator==(const Completion&) const = default;
};

struct History {
    std::vector<Invocation> invocations;
    std::vector<Completion> completions;

    bool operator==(const History&) const = default;
};

struct CheckerLimits {
    std::size_t max_operations{64};
    std::size_t max_search_states{200'000};
    std::size_t max_memo_entries{100'000};
};

enum class CheckOutcome {
    linearizable,
    not_linearizable,
    inconclusive
};

struct Counterexample {
    History history;
    std::string replay;
};

struct CheckResult {
    CheckOutcome outcome{CheckOutcome::inconclusive};
    std::size_t explored_states{};
    std::optional<Counterexample> counterexample;
};

class InvalidHistory final : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

[[nodiscard]] CheckResult check(
    const History& history,
    CheckerLimits limits = {});

[[nodiscard]] std::string serialize(const History& history);
[[nodiscard]] History parse(std::string_view replay);

}  // namespace kura::metadata::testing::linearizability
