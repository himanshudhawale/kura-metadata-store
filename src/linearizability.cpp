#include "kura/metadata/testing/linearizability.hpp"

#include <algorithm>
#include <charconv>
#include <iomanip>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace kura::metadata::testing::linearizability {
namespace {

struct ModelState {
    Revision revision;
    std::map<ByteSequence, VersionedValue> values;
};

struct SearchOperation {
    Invocation invocation;
    std::optional<Completion> completion;
    bool required{};
    std::uint64_t predecessors{};
};

enum class SearchOutcome {
    linearizable,
    not_linearizable,
    inconclusive
};

struct SearchContext {
    std::vector<SearchOperation> operations;
    CheckerLimits limits;
    std::size_t explored{};
    std::unordered_set<std::string> dead_states;
};

[[nodiscard]] std::string hex(const ByteSequence& value) {
    if (value.empty()) {
        return "-";
    }
    static constexpr char digits[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (const auto byte : value.bytes()) {
        encoded.push_back(digits[byte >> 4]);
        encoded.push_back(digits[byte & 0x0f]);
    }
    return encoded;
}

[[nodiscard]] ByteSequence unhex(const std::string_view text) {
    if (text == "-") {
        return {};
    }
    if (text.empty() || text.size() % 2 != 0) {
        throw InvalidHistory("invalid byte sequence in replay");
    }
    std::vector<std::uint8_t> bytes;
    bytes.reserve(text.size() / 2);
    auto nibble = [](const char value) -> std::uint8_t {
        if (value >= '0' && value <= '9') {
            return static_cast<std::uint8_t>(value - '0');
        }
        if (value >= 'a' && value <= 'f') {
            return static_cast<std::uint8_t>(value - 'a' + 10);
        }
        throw InvalidHistory("invalid hexadecimal digit in replay");
    };
    for (std::size_t offset = 0; offset < text.size(); offset += 2) {
        bytes.push_back(static_cast<std::uint8_t>(
            (nibble(text[offset]) << 4) | nibble(text[offset + 1])));
    }
    return ByteSequence(std::move(bytes));
}

template <typename Integer>
[[nodiscard]] Integer integer(
    const std::string_view token,
    const std::string_view field) {
    Integer value{};
    const auto [end, error] = std::from_chars(
        token.data(), token.data() + token.size(), value);
    if (error != std::errc{} || end != token.data() + token.size()) {
        throw InvalidHistory("invalid " + std::string(field) + " in replay");
    }
    return value;
}

[[nodiscard]] Revision revision(const std::string_view token) {
    const auto value = integer<std::int64_t>(token, "revision");
    if (value < 0) {
        throw InvalidHistory("negative revision in replay");
    }
    return {value};
}

[[nodiscard]] bool boolean(const std::string_view token) {
    if (token == "0") {
        return false;
    }
    if (token == "1") {
        return true;
    }
    throw InvalidHistory("invalid boolean in replay");
}

[[nodiscard]] bool operation_matches_result(
    const Operation& operation,
    const Result& result) {
    return (std::holds_alternative<Get>(operation)
            && std::holds_alternative<GetResult>(result))
        || (std::holds_alternative<Put>(operation)
            && std::holds_alternative<PutResult>(result))
        || (std::holds_alternative<Erase>(operation)
            && std::holds_alternative<EraseResult>(result))
        || (std::holds_alternative<CompareAndSwap>(operation)
            && std::holds_alternative<CompareAndSwapResult>(result));
}

void validate(const History& history) {
    std::map<OperationId, const Invocation*> invocations;
    for (const auto& invocation : history.invocations) {
        if (invocation.operation_id == 0 || invocation.client_id == 0) {
            throw InvalidHistory("operation and client IDs must be nonzero");
        }
        if (!invocations.emplace(
                invocation.operation_id, &invocation).second) {
            throw InvalidHistory("duplicate invocation operation ID");
        }
        std::visit(
            [](const auto& operation) {
                if (operation.key.empty()) {
                    throw InvalidHistory("operation key must be nonempty");
                }
            },
            invocation.operation);
    }

    std::set<OperationId> completions;
    for (const auto& completion : history.completions) {
        const auto invocation = invocations.find(completion.operation_id);
        if (invocation == invocations.end()) {
            throw InvalidHistory("completion has no invocation");
        }
        if (!completions.insert(completion.operation_id).second) {
            throw InvalidHistory("duplicate completion operation ID");
        }
        if (completion.completed_at < invocation->second->invoked_at) {
            throw InvalidHistory("completion precedes invocation");
        }
        if (completion.kind == CompletionKind::succeeded) {
            if (!completion.result
                || !operation_matches_result(
                    invocation->second->operation, *completion.result)) {
                throw InvalidHistory(
                    "successful completion has a mismatched result");
            }
            std::visit(
                [](const auto& result) {
                    if (result.revision.value < 0) {
                        throw InvalidHistory(
                            "result revision must be nonnegative");
                    }
                    using Concrete = std::decay_t<decltype(result)>;
                    if constexpr (std::is_same_v<Concrete, GetResult>) {
                        if (result.value
                            && (result.value->modification_revision.value
                                    <= 0
                                || result.value
                                       ->modification_revision
                                    > result.revision)) {
                            throw InvalidHistory(
                                "get value revision is invalid");
                        }
                    }
                },
                *completion.result);
        } else if (completion.result) {
            throw InvalidHistory(
                "non-successful completion must not carry a result");
        }
    }
}

[[nodiscard]] bool current_equals(
    const ModelState& state,
    const ByteSequence& key,
    const std::optional<ByteSequence>& expected) {
    const auto found = state.values.find(key);
    if (!expected) {
        return found == state.values.end();
    }
    return found != state.values.end()
        && found->second.value == *expected;
}

[[nodiscard]] std::optional<Revision> next_revision(
    const Revision current) {
    if (current.value == std::numeric_limits<std::int64_t>::max()) {
        return std::nullopt;
    }
    return Revision{current.value + 1};
}

[[nodiscard]] bool apply(
    ModelState& state,
    const Operation& operation,
    const std::optional<Result>& expected_result) {
    return std::visit(
        [&state, &expected_result](const auto& concrete) {
            using Concrete = std::decay_t<decltype(concrete)>;
            if constexpr (std::is_same_v<Concrete, Get>) {
                GetResult actual{.revision = state.revision};
                const auto found = state.values.find(concrete.key);
                if (found != state.values.end()) {
                    actual.value = found->second;
                }
                return !expected_result
                    || std::get<GetResult>(*expected_result) == actual;
            } else if constexpr (std::is_same_v<Concrete, Put>) {
                const auto next = next_revision(state.revision);
                if (!next) {
                    return false;
                }
                const PutResult actual{*next};
                if (expected_result
                    && std::get<PutResult>(*expected_result) != actual) {
                    return false;
                }
                state.revision = *next;
                state.values[concrete.key] = {
                    concrete.value, *next};
                return true;
            } else if constexpr (std::is_same_v<Concrete, Erase>) {
                const auto found = state.values.find(concrete.key);
                EraseResult actual{
                    .erased = found != state.values.end(),
                    .revision = state.revision};
                if (actual.erased) {
                    const auto next = next_revision(state.revision);
                    if (!next) {
                        return false;
                    }
                    actual.revision = *next;
                }
                if (expected_result
                    && std::get<EraseResult>(*expected_result) != actual) {
                    return false;
                }
                if (actual.erased) {
                    state.values.erase(found);
                    state.revision = actual.revision;
                }
                return true;
            } else {
                const bool exchanged = current_equals(
                    state, concrete.key, concrete.expected);
                CompareAndSwapResult actual{
                    .exchanged = exchanged,
                    .revision = state.revision};
                bool mutates = false;
                if (exchanged) {
                    const auto found = state.values.find(concrete.key);
                    mutates = concrete.desired.has_value()
                        || found != state.values.end();
                    if (mutates) {
                        const auto next = next_revision(state.revision);
                        if (!next) {
                            return false;
                        }
                        actual.revision = *next;
                    }
                }
                if (expected_result
                    && std::get<CompareAndSwapResult>(*expected_result)
                        != actual) {
                    return false;
                }
                if (!exchanged || !mutates) {
                    return true;
                }
                state.revision = actual.revision;
                if (concrete.desired) {
                    state.values[concrete.key] = {
                        *concrete.desired, actual.revision};
                } else {
                    state.values.erase(concrete.key);
                }
                return true;
            }
        },
        operation);
}

[[nodiscard]] std::string state_key(
    const std::uint64_t remaining,
    const ModelState& state) {
    std::string key = std::to_string(remaining);
    key.push_back('|');
    key.append(std::to_string(state.revision.value));
    for (const auto& [entry_key, value] : state.values) {
        key.push_back('|');
        key.append(hex(entry_key));
        key.push_back('=');
        key.append(hex(value.value));
        key.push_back('@');
        key.append(std::to_string(value.modification_revision.value));
    }
    return key;
}

[[nodiscard]] SearchOutcome search(
    SearchContext& context,
    const std::uint64_t remaining,
    const ModelState& state) {
    if (remaining == 0) {
        return SearchOutcome::linearizable;
    }
    if (context.explored >= context.limits.max_search_states) {
        return SearchOutcome::inconclusive;
    }
    ++context.explored;
    const auto memo = state_key(remaining, state);
    if (context.dead_states.contains(memo)) {
        return SearchOutcome::not_linearizable;
    }
    if (context.dead_states.size() >= context.limits.max_memo_entries) {
        return SearchOutcome::inconclusive;
    }

    bool saw_inconclusive = false;
    for (std::size_t index = 0; index < context.operations.size(); ++index) {
        const auto bit = std::uint64_t{1} << index;
        if ((remaining & bit) == 0) {
            continue;
        }
        const auto& candidate = context.operations[index];
        if ((candidate.predecessors & remaining) != 0) {
            continue;
        }
        const auto after = remaining & ~bit;
        if (!candidate.required) {
            const auto omitted = search(context, after, state);
            if (omitted == SearchOutcome::linearizable) {
                return omitted;
            }
            saw_inconclusive =
                saw_inconclusive
                || omitted == SearchOutcome::inconclusive;
        }

        auto next = state;
        const auto expected = candidate.required
            ? candidate.completion->result
            : std::optional<Result>{};
        if (!apply(next, candidate.invocation.operation, expected)) {
            continue;
        }
        const auto applied = search(context, after, next);
        if (applied == SearchOutcome::linearizable) {
            return applied;
        }
        saw_inconclusive =
            saw_inconclusive
            || applied == SearchOutcome::inconclusive;
    }
    if (saw_inconclusive) {
        return SearchOutcome::inconclusive;
    }
    context.dead_states.insert(memo);
    return SearchOutcome::not_linearizable;
}

struct InternalResult {
    SearchOutcome outcome;
    std::size_t explored{};
};

[[nodiscard]] InternalResult check_internal(
    const History& history,
    const CheckerLimits limits) {
    validate(history);
    if (limits.max_operations == 0 || limits.max_search_states == 0
        || limits.max_memo_entries == 0) {
        return {SearchOutcome::inconclusive, 0};
    }
    if (history.invocations.size() > limits.max_operations) {
        return {SearchOutcome::inconclusive, 0};
    }

    std::map<OperationId, Completion> completions;
    for (const auto& completion : history.completions) {
        completions.emplace(completion.operation_id, completion);
    }
    SearchContext context{.limits = limits};
    for (const auto& invocation : history.invocations) {
        const auto found = completions.find(invocation.operation_id);
        if (found != completions.end()
            && (found->second.kind == CompletionKind::failed
                || found->second.kind == CompletionKind::timed_out)) {
            continue;
        }
        const bool required = found != completions.end()
            && found->second.kind == CompletionKind::succeeded;
        const bool mutating =
            !std::holds_alternative<Get>(invocation.operation);
        if (!required && !mutating) {
            continue;
        }
        context.operations.push_back({
            .invocation = invocation,
            .completion =
                found == completions.end()
                    ? std::nullopt
                    : std::optional<Completion>{found->second},
            .required = required});
    }
    std::ranges::sort(
        context.operations,
        {},
        [](const SearchOperation& operation) {
            return operation.invocation.operation_id;
        });
    if (context.operations.size() > 64) {
        return {SearchOutcome::inconclusive, 0};
    }

    for (std::size_t later = 0; later < context.operations.size(); ++later) {
        for (std::size_t earlier = 0;
             earlier < context.operations.size();
             ++earlier) {
            if (earlier == later
                || !context.operations[earlier].completion) {
                continue;
            }
            if (context.operations[earlier].completion->completed_at
                < context.operations[later].invocation.invoked_at) {
                context.operations[later].predecessors |=
                    std::uint64_t{1} << earlier;
            }
        }
    }
    const auto count = context.operations.size();
    const auto remaining = count == 64
        ? std::numeric_limits<std::uint64_t>::max()
        : (std::uint64_t{1} << count) - 1;
    const auto outcome = search(context, remaining, {});
    return {outcome, context.explored};
}

[[nodiscard]] History without(
    const History& history,
    const OperationId operation_id) {
    History reduced;
    std::ranges::copy_if(
        history.invocations,
        std::back_inserter(reduced.invocations),
        [operation_id](const Invocation& invocation) {
            return invocation.operation_id != operation_id;
        });
    std::ranges::copy_if(
        history.completions,
        std::back_inserter(reduced.completions),
        [operation_id](const Completion& completion) {
            return completion.operation_id != operation_id;
        });
    return reduced;
}

[[nodiscard]] std::string completion_kind_name(
    const CompletionKind kind) {
    switch (kind) {
        case CompletionKind::succeeded:
            return "succeeded";
        case CompletionKind::failed:
            return "failed";
        case CompletionKind::timed_out:
            return "timed_out";
        case CompletionKind::indeterminate:
            return "indeterminate";
    }
    throw std::logic_error("unknown completion kind");
}

}  // namespace

CheckResult check(const History& history, const CheckerLimits limits) {
    const auto result = check_internal(history, limits);
    CheckResult checked{.explored_states = result.explored};
    if (result.outcome == SearchOutcome::linearizable) {
        checked.outcome = CheckOutcome::linearizable;
        return checked;
    }
    if (result.outcome == SearchOutcome::inconclusive) {
        checked.outcome = CheckOutcome::inconclusive;
        return checked;
    }

    checked.outcome = CheckOutcome::not_linearizable;
    auto reduced = history;
    bool changed = true;
    while (changed) {
        changed = false;
        std::vector<OperationId> ids;
        ids.reserve(reduced.invocations.size());
        for (const auto& invocation : reduced.invocations) {
            ids.push_back(invocation.operation_id);
        }
        std::ranges::sort(ids);
        for (const auto id : ids) {
            const auto candidate = without(reduced, id);
            if (candidate.invocations.empty()) {
                continue;
            }
            const auto candidate_result = check_internal(candidate, limits);
            checked.explored_states += candidate_result.explored;
            if (candidate_result.outcome
                == SearchOutcome::not_linearizable) {
                reduced = candidate;
                changed = true;
                break;
            }
        }
    }
    checked.counterexample = Counterexample{
        .history = reduced,
        .replay = serialize(reduced)};
    return checked;
}

std::string serialize(const History& history) {
    validate(history);
    auto invocations = history.invocations;
    auto completions = history.completions;
    std::ranges::sort(
        invocations, {}, &Invocation::operation_id);
    std::ranges::sort(
        completions, {}, &Completion::operation_id);

    std::ostringstream output;
    output << "linear-history-v1\n";
    for (const auto& invocation : invocations) {
        output << "i " << invocation.operation_id << ' '
               << invocation.client_id << ' ' << invocation.invoked_at << ' ';
        std::visit(
            [&output](const auto& operation) {
                using Concrete = std::decay_t<decltype(operation)>;
                if constexpr (std::is_same_v<Concrete, Get>) {
                    output << "get " << hex(operation.key);
                } else if constexpr (std::is_same_v<Concrete, Put>) {
                    output << "put " << hex(operation.key) << ' '
                           << hex(operation.value);
                } else if constexpr (std::is_same_v<Concrete, Erase>) {
                    output << "erase " << hex(operation.key);
                } else {
                    output << "cas " << hex(operation.key) << ' '
                           << (operation.expected
                                   ? hex(*operation.expected)
                                   : "~")
                           << ' '
                           << (operation.desired
                                   ? hex(*operation.desired)
                                   : "~");
                }
            },
            invocation.operation);
        output << '\n';
    }
    for (const auto& completion : completions) {
        output << "c " << completion.operation_id << ' '
               << completion.completed_at << ' '
               << completion_kind_name(completion.kind);
        if (completion.kind == CompletionKind::succeeded) {
            std::visit(
                [&output](const auto& result) {
                    using Concrete = std::decay_t<decltype(result)>;
                    if constexpr (std::is_same_v<Concrete, GetResult>) {
                        output << " get " << result.revision.value << ' '
                               << (result.value ? 1 : 0);
                        if (result.value) {
                            output << ' ' << hex(result.value->value)
                                   << ' '
                                   << result.value
                                          ->modification_revision.value;
                        }
                    } else if constexpr (
                        std::is_same_v<Concrete, PutResult>) {
                        output << " put " << result.revision.value;
                    } else if constexpr (
                        std::is_same_v<Concrete, EraseResult>) {
                        output << " erase " << (result.erased ? 1 : 0)
                               << ' ' << result.revision.value;
                    } else {
                        output << " cas " << (result.exchanged ? 1 : 0)
                               << ' ' << result.revision.value;
                    }
                },
                *completion.result);
        }
        output << '\n';
    }
    return output.str();
}

History parse(const std::string_view replay) {
    std::istringstream input{std::string(replay)};
    std::string line;
    if (!std::getline(input, line) || line != "linear-history-v1") {
        throw InvalidHistory("unsupported linearizability replay header");
    }
    History history;
    while (std::getline(input, line)) {
        if (line.empty()) {
            throw InvalidHistory("blank line in replay");
        }
        std::istringstream fields(line);
        std::vector<std::string> tokens;
        for (std::string token; fields >> token;) {
            tokens.push_back(std::move(token));
        }
        if (tokens.empty()) {
            throw InvalidHistory("empty replay record");
        }
        if (tokens[0] == "i") {
            if (tokens.size() < 5) {
                throw InvalidHistory("truncated invocation replay record");
            }
            Invocation invocation{
                .operation_id =
                    integer<OperationId>(tokens[1], "operation ID"),
                .client_id = integer<ClientId>(tokens[2], "client ID"),
                .invoked_at =
                    integer<LogicalTime>(tokens[3], "invocation time")};
            if (tokens[4] == "get" && tokens.size() == 6) {
                invocation.operation = Get{unhex(tokens[5])};
            } else if (tokens[4] == "put" && tokens.size() == 7) {
                invocation.operation =
                    Put{unhex(tokens[5]), unhex(tokens[6])};
            } else if (tokens[4] == "erase" && tokens.size() == 6) {
                invocation.operation = Erase{unhex(tokens[5])};
            } else if (tokens[4] == "cas" && tokens.size() == 8) {
                invocation.operation = CompareAndSwap{
                    .key = unhex(tokens[5]),
                    .expected =
                        tokens[6] == "~"
                            ? std::nullopt
                            : std::optional<ByteSequence>{
                                  unhex(tokens[6])},
                    .desired =
                        tokens[7] == "~"
                            ? std::nullopt
                            : std::optional<ByteSequence>{
                                  unhex(tokens[7])}};
            } else {
                throw InvalidHistory("invalid invocation replay record");
            }
            history.invocations.push_back(std::move(invocation));
            continue;
        }
        if (tokens[0] != "c" || tokens.size() < 4) {
            throw InvalidHistory("invalid completion replay record");
        }
        Completion completion{
            .operation_id =
                integer<OperationId>(tokens[1], "operation ID"),
            .completed_at =
                integer<LogicalTime>(tokens[2], "completion time")};
        if (tokens[3] == "failed" || tokens[3] == "timed_out"
            || tokens[3] == "indeterminate") {
            if (tokens.size() != 4) {
                throw InvalidHistory(
                    "non-success completion has replay result");
            }
            completion.kind = tokens[3] == "failed"
                ? CompletionKind::failed
                : (tokens[3] == "timed_out"
                       ? CompletionKind::timed_out
                       : CompletionKind::indeterminate);
        } else if (tokens[3] == "succeeded") {
            completion.kind = CompletionKind::succeeded;
            if (tokens.size() >= 7 && tokens[4] == "get") {
                const auto present = boolean(tokens[6]);
                if ((!present && tokens.size() != 7)
                    || (present && tokens.size() != 9)) {
                    throw InvalidHistory("invalid get completion replay");
                }
                GetResult result{.revision = revision(tokens[5])};
                if (present) {
                    result.value = VersionedValue{
                        unhex(tokens[7]), revision(tokens[8])};
                }
                completion.result = std::move(result);
            } else if (tokens.size() == 6 && tokens[4] == "put") {
                completion.result = PutResult{revision(tokens[5])};
            } else if (tokens.size() == 7 && tokens[4] == "erase") {
                completion.result = EraseResult{
                    boolean(tokens[5]), revision(tokens[6])};
            } else if (tokens.size() == 7 && tokens[4] == "cas") {
                completion.result = CompareAndSwapResult{
                    boolean(tokens[5]), revision(tokens[6])};
            } else {
                throw InvalidHistory("invalid success completion replay");
            }
        } else {
            throw InvalidHistory("unknown completion kind in replay");
        }
        history.completions.push_back(std::move(completion));
    }
    validate(history);
    return history;
}

}  // namespace kura::metadata::testing::linearizability
