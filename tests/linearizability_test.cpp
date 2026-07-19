#include "kura/metadata/testing/linearizability.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

namespace {

using namespace kura::metadata;
using namespace kura::metadata::testing::linearizability;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename Function>
void expect_invalid(Function&& function, const std::string& message) {
    try {
        function();
    } catch (const InvalidHistory&) {
        return;
    }
    throw TestFailure(message);
}

ByteSequence bytes(const std::string_view value) {
    return ByteSequence::from_string(value);
}

Invocation invocation(
    const OperationId id,
    const ClientId client,
    const LogicalTime time,
    Operation operation) {
    return {id, client, time, std::move(operation)};
}

Completion success(
    const OperationId id,
    const LogicalTime time,
    Result result) {
    return {
        .operation_id = id,
        .completed_at = time,
        .kind = CompletionKind::succeeded,
        .result = std::move(result)};
}

void sequential_metadata_history_is_linearizable() {
    const auto key = bytes("catalog/table");
    const auto first = bytes("manifest-a");
    const auto second = bytes("manifest-b");
    History history{
        .invocations = {
            invocation(1, 1, 1, Put{key, first}),
            invocation(2, 2, 4, Get{key}),
            invocation(
                3,
                1,
                7,
                CompareAndSwap{key, first, second}),
            invocation(4, 2, 10, Get{key}),
            invocation(5, 1, 13, Erase{key}),
            invocation(6, 2, 16, Get{key})},
        .completions = {
            success(1, 3, PutResult{{1}}),
            success(
                2,
                6,
                GetResult{
                    VersionedValue{first, {1}},
                    {1}}),
            success(3, 9, CompareAndSwapResult{true, {2}}),
            success(
                4,
                12,
                GetResult{
                    VersionedValue{second, {2}},
                    {2}}),
            success(5, 15, EraseResult{true, {3}}),
            success(6, 18, GetResult{std::nullopt, {3}})}};
    const auto checked = check(history);
    expect(
        checked.outcome == CheckOutcome::linearizable
            && !checked.counterexample,
        "valid put/get/CAS/erase history was rejected");
    const auto replay = serialize(history);
    expect(
        parse(replay) == history
            && serialize(parse(replay)) == replay,
        "history serialization was not deterministic and replayable");
}

void concurrent_operations_search_for_a_valid_order() {
    const auto key = bytes("key");
    History history{
        .invocations = {
            invocation(10, 1, 1, Put{key, bytes("a")}),
            invocation(11, 2, 1, Put{key, bytes("b")}),
            invocation(12, 3, 2, Get{key})},
        .completions = {
            success(10, 5, PutResult{{1}}),
            success(11, 4, PutResult{{2}}),
            success(
                12,
                3,
                GetResult{
                    VersionedValue{bytes("a"), {1}},
                    {1}})}};
    expect(
        check(history).outcome == CheckOutcome::linearizable,
        "checker ignored a valid concurrent linearization");
}

void failed_cas_and_absent_erase_do_not_advance_revision() {
    const auto key = bytes("key");
    const auto other = bytes("other");
    History history{
        .invocations = {
            invocation(1, 1, 1, Put{key, bytes("a")}),
            invocation(
                2,
                1,
                3,
                CompareAndSwap{key, bytes("wrong"), bytes("b")}),
            invocation(3, 1, 5, Erase{other}),
            invocation(4, 1, 7, Get{key})},
        .completions = {
            success(1, 2, PutResult{{1}}),
            success(2, 4, CompareAndSwapResult{false, {1}}),
            success(3, 6, EraseResult{false, {1}}),
            success(
                4,
                8,
                GetResult{
                    VersionedValue{bytes("a"), {1}},
                    {1}})}};
    expect(
        check(history).outcome == CheckOutcome::linearizable,
        "failed CAS or absent erase changed the model revision");
}

void uncertainty_is_distinguished_without_fabricating_success() {
    const auto key = bytes("key");
    History history{
        .invocations = {
            invocation(1, 1, 1, Put{key, bytes("failed")}),
            invocation(2, 1, 3, Put{key, bytes("timed")}),
            invocation(3, 2, 5, Put{key, bytes("maybe")}),
            invocation(4, 3, 6, Get{key})},
        .completions = {
            {1, 2, CompletionKind::failed, std::nullopt},
            {2, 4, CompletionKind::timed_out, std::nullopt},
            {3, 8, CompletionKind::indeterminate, std::nullopt},
            success(
                4,
                9,
                GetResult{
                    VersionedValue{bytes("maybe"), {1}},
                    {1}})}};
    expect(
        check(history).outcome == CheckOutcome::linearizable,
        "indeterminate mutation could not explain an observed value");

    history.completions[2].kind = CompletionKind::failed;
    expect(
        check(history).outcome == CheckOutcome::not_linearizable,
        "definite failure was allowed to mutate state");

    history.completions.erase(history.completions.begin() + 2);
    expect(
        check(history).outcome == CheckOutcome::linearizable,
        "pending mutation could not be linearized");
}

void violations_produce_minimal_replayable_counterexamples() {
    const auto key = bytes("key");
    const History history{
        .invocations = {
            invocation(1, 1, 1, Put{key, bytes("new")}),
            invocation(2, 2, 4, Get{key}),
            invocation(3, 3, 7, Put{bytes("other"), bytes("noise")})},
        .completions = {
            success(1, 3, PutResult{{1}}),
            success(2, 6, GetResult{std::nullopt, {0}}),
            {3, 8, CompletionKind::failed, std::nullopt}}};
    const auto checked = check(history);
    expect(
        checked.outcome == CheckOutcome::not_linearizable
            && checked.counterexample
            && checked.counterexample->history.invocations.size() == 2,
        "violation did not produce a one-minimal counterexample");
    const auto replayed = parse(checked.counterexample->replay);
    expect(
        check(replayed).outcome == CheckOutcome::not_linearizable
            && serialize(replayed) == checked.counterexample->replay,
        "counterexample was not deterministic and replayable");
}

void resource_limits_are_explicitly_inconclusive() {
    const auto key = bytes("key");
    History history{
        .invocations = {
            invocation(1, 1, 1, Put{key, bytes("a")}),
            invocation(2, 2, 1, Put{key, bytes("b")})},
        .completions = {
            success(1, 3, PutResult{{1}}),
            success(2, 3, PutResult{{2}})}};
    const auto checked = check(
        history,
        {
            .max_operations = 1,
            .max_search_states = 100,
            .max_memo_entries = 100});
    expect(
        checked.outcome == CheckOutcome::inconclusive
            && !checked.counterexample,
        "resource exhaustion was reported as success or violation");
}

void malformed_histories_fail_closed() {
    const auto key = bytes("key");
    const auto valid_invocation = invocation(1, 1, 2, Get{key});
    const auto valid_completion =
        success(1, 3, GetResult{std::nullopt, {0}});

    expect_invalid(
        [&] {
            static_cast<void>(check({
                .invocations = {valid_invocation, valid_invocation},
                .completions = {valid_completion}}));
        },
        "duplicate invocation was accepted");
    expect_invalid(
        [&] {
            auto orphan = valid_completion;
            orphan.operation_id = 2;
            static_cast<void>(check({
                .invocations = {valid_invocation},
                .completions = {orphan}}));
        },
        "orphan completion was accepted");
    expect_invalid(
        [&] {
            auto early = valid_completion;
            early.completed_at = 1;
            static_cast<void>(check({
                .invocations = {valid_invocation},
                .completions = {early}}));
        },
        "completion before invocation was accepted");
    expect_invalid(
        [&] {
            static_cast<void>(check({
                .invocations = {valid_invocation},
                .completions = {
                    success(1, 3, PutResult{{1}})}}));
        },
        "mismatched result type was accepted");
    expect_invalid(
        [&] {
            auto failed = Completion{
                1,
                3,
                CompletionKind::failed,
                GetResult{std::nullopt, {0}}};
            static_cast<void>(check({
                .invocations = {valid_invocation},
                .completions = {failed}}));
        },
        "failed completion with a result was accepted");
    expect_invalid(
        [&] {
            static_cast<void>(check({
                .invocations = {
                    invocation(1, 1, 1, Get{ByteSequence{}})}}));
        },
        "empty key was accepted");
    expect_invalid(
        [&] {
            static_cast<void>(parse("linear-history-v1\nc 1 2 failed\n"));
        },
        "malformed replay was accepted");
}

}  // namespace

int main() {
    try {
        sequential_metadata_history_is_linearizable();
        concurrent_operations_search_for_a_valid_order();
        failed_cas_and_absent_erase_do_not_advance_revision();
        uncertainty_is_distinguished_without_fabricating_success();
        violations_produce_minimal_replayable_counterexamples();
        resource_limits_are_explicitly_inconclusive();
        malformed_histories_fail_closed();
        std::cout << "linearizability tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "linearizability test failure: "
                  << error.what() << '\n';
        return 1;
    }
}
