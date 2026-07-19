#include "kura/metadata/byte_sequence.hpp"

#include <utility>

namespace kura::metadata {

ByteSequence::ByteSequence(std::vector<std::uint8_t> bytes)
    : bytes_(std::move(bytes)) {}

ByteSequence ByteSequence::copy_from(
    const std::span<const std::uint8_t> bytes) {
    return ByteSequence({bytes.begin(), bytes.end()});
}

ByteSequence ByteSequence::from_string(const std::string_view value) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(value.size());
    for (const char character : value) {
        bytes.push_back(static_cast<std::uint8_t>(
            static_cast<unsigned char>(character)));
    }
    return ByteSequence(std::move(bytes));
}

std::span<const std::uint8_t> ByteSequence::bytes() const noexcept {
    return bytes_;
}

std::size_t ByteSequence::size() const noexcept {
    return bytes_.size();
}

bool ByteSequence::empty() const noexcept {
    return bytes_.empty();
}

std::string ByteSequence::to_string() const {
    std::string value;
    value.reserve(bytes_.size());
    for (const std::uint8_t byte : bytes_) {
        value.push_back(static_cast<char>(byte));
    }
    return value;
}

}  // namespace kura::metadata
