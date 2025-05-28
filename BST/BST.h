#pragma once
#include <cassert>
#include <array>
#include <iostream>
#include <memory>
#include <utility>
#include "../bitset_wrapper/bitset_wrapper.h"
#include "../config/config.h"

struct Node {
    int index{};
    std::unique_ptr<Node> left;
    std::unique_ptr<Node> right;
};

#ifdef DEBUG
// Helper function to get child type
std::string getChildType(const Node* node) {
    if (node->left && node->right) return "00"; // Both children are non-null
    if (!node->left && !node->right) return "11"; // Both children are null
    if (node->left) return "01"; // Only left is non-null
    return "10"; // Only right is non-null
}

// Recursive function to print the tree in a tree-like format
void printTreeLikeStructure(const Node* node, const std::string& prefix = "", bool isLeft = true) {
    if (!node) return; // Base case: if the node is null, return

    std::cout << prefix;

    // Print node branch
    std::cout << (isLeft ? "├── " : "└── ");

    // Print the node index and its child type
    std::cout << "Index: " << node->index << " (" << getChildType(node) << ")\n";

    // Recursively print left and right children with adjusted prefix
    std::string newPrefix = prefix + (isLeft ? "│   " : "    ");
    printTreeLikeStructure(node->left.get(), newPrefix, true);
    printTreeLikeStructure(node->right.get(), newPrefix, false);
}

// Function to start printing the tree
void printTree(const Node* root) {
    if (!root) {
        return;
    }
    std::cout << std::endl;
    printTreeLikeStructure(root);
}
#endif

template <size_t LENGTH>
struct Rep {
    BitsetWrapper<LENGTH> bw;
    size_t firstInvalidIndex;
};
#pragma pack(push, 1)  // Ensure no automatic padding is added
struct ExpandedLSlot {
    BitsetWrapper<COUNT_SLOT> bw;
    uint32_t ten : 4;
    uint32_t firstInvalidIndex : 6;
    uint32_t wasExtended : 1;
    uint32_t orgBlock : 7;
    uint32_t orgLSlot : 7;
    uint32_t payload_start : 7;

    void set(size_t ten_, size_t orgBlock_, size_t orgLSlot_, size_t payload_start_, bool wasExtended_, Rep<REGISTER_SIZE> lslot_rep_) {
        ten = ten_;
        orgBlock = orgBlock_;
        orgLSlot = orgLSlot_;
        payload_start = payload_start_;
        wasExtended = wasExtended_;
        firstInvalidIndex = lslot_rep_.firstInvalidIndex;
        bw = lslot_rep_.bw;
    }

    bool isDefault() const {
        return ten == 0;  // && orgBlock == 0 && orgLSlot == 0 && payload_start == 0 && !wasExtended;
    }

    void print() const {
        if (isDefault()) {
            // std::cout << "Default Slot\n";
        } else {
            std::cout << "ten: " << ten << "\n"
                      << "orgBlock: " << orgBlock << "\n"
                      << "orgLSlot: " << orgLSlot << "\n"
                      << "payload_start: " << payload_start << "\n"
                      << "wasExtended: " << (wasExtended ? "true" : "false") << "\n"
                      << "lslot_rep_size: " << firstInvalidIndex << "\n"   // Assuming Rep<REGISTER_SIZE> has a toString() method
                      << "lslot_rep_bw: " << bw.getInputString() << "\n";  // Assuming Rep<REGISTER_SIZE> has a toString() method
        }
    }
    size_t get_count() const {
        return ten;
    }
};
#pragma pack(pop)
class ExpandedBlock {
   public:
    std::array<ExpandedLSlot, COUNT_SLOT> lslots;

    void print() const {
        std::cout << "printing new block\n";
        for (const auto& slot : lslots) {
            slot.print();
            std::cout << "-------------------\n";
        }
    }
    size_t get_count() const {
        size_t sum = 0;
        for (const auto& slot : lslots) {
            sum += slot.get_count();
        }
        return sum;
    }
};

class ExpandedSegment {
   public:
    std::array<ExpandedBlock, COUNT_SLOT> blocks;
    void print() const {
        std::cout << "printing new segment\n";
        for (const auto& block : blocks) {
            block.print();
            std::cout << "===================\n";
        }
    }
    size_t get_count() const {
        size_t sum = 0;
        for (const auto& block : blocks) {
            sum += block.get_count();
        }
        return sum;
    }
};

template <size_t LENGTH>
class BST {
   private:
    size_t startingIndex;
    size_t tenSize;
    size_t FPIndex;

    size_t createBSTFor3orMore(const BitsetWrapper<LENGTH>& bw, const size_t ten_remaining, size_t& index, const std::unique_ptr<Node>& node, const int prev_count) {
        size_t index_bits = 0b11;
        if (ten_remaining != 2) {
            index_bits = bw.range(index, index + 2);
            index += 2;
        }
        int count = 1;
        bw.count_contiguous(index, count);

        node->index = prev_count + count;
        if (index_bits == 0b11) {
            return ten_remaining - 2;
        } else if (index_bits == 0b10) {
            node->right = std::make_unique<Node>();
            auto ten_remaining_local = createBSTFor3orMore(bw, ten_remaining - 1, index, node->right, node->index);
            return ten_remaining_local;
        } else if (index_bits == 0b01) {
            node->left = std::make_unique<Node>();
            auto ten_remaining_local = createBSTFor3orMore(bw, ten_remaining - 1, index, node->left, node->index);
            return ten_remaining_local;
        } else if (index_bits == 0b00) {
            node->left = std::make_unique<Node>();
            node->right = std::make_unique<Node>();
            auto ten_remaining_local = createBSTFor3orMore(bw, ten_remaining, index, node->left, node->index);
            ten_remaining_local = createBSTFor3orMore(bw, ten_remaining_local, index, node->right, node->index);
            return ten_remaining_local;
        } else {
            throw std::invalid_argument("Should not be here");
        }
    }

    void setContiguous(const size_t count, size_t& index, BitsetWrapper<REGISTER_SIZE>& bw) {
        for (size_t counter = 1; counter < count; counter++) {
            bw.set(index++ - startingIndex, false);
        }
        bw.set(index++ - startingIndex, true);
    }

    [[nodiscard]] size_t count(const std::unique_ptr<Node>& node) const {
        if (!node)
            return 1;
        return count(node->left) + count(node->right);
    }

    size_t _getOffsetIdx(BitsetWrapper<FINGERPRINT_SIZE>& fp, const std::unique_ptr<Node>& node) const {
        if (!node)
            return 0;
        const auto idx = node->index;
        if (fp.get(idx))
            return count(node->left) + _getOffsetIdx(fp, node->right);
        return _getOffsetIdx(fp, node->left);
    }

   public:
    std::unique_ptr<Node> root = nullptr;
    explicit BST(const size_t tenSize_, const size_t startingIndex_ = 0, const size_t FPIndex_ = 0) : startingIndex(startingIndex_), tenSize(tenSize_), FPIndex(FPIndex_) {}
    static size_t get_first_diff_index(const BitsetWrapper<FINGERPRINT_SIZE>& old_fp, const BitsetWrapper<FINGERPRINT_SIZE>& new_fp) {
        return (old_fp ^ new_fp).get_trailing_zeros(0);
    }

    size_t getTen(const std::unique_ptr<Node>& node) {
        if (!node) {
            return 1;  // only count itself
        }
        return getTen(node->left) + getTen(node->right);
    }

    size_t getTenSize() const {
        return tenSize;
    }
    size_t getFPIndex() const {
        return FPIndex;
    }
    void createBST(const BitsetWrapper<LENGTH>& bw) {
        if (tenSize == 0 || tenSize == 1) {
            root = nullptr;
            return;
        }
        size_t index = startingIndex;
        if (tenSize == 2) {
            int count = 0;
            bw.count_contiguous(index, count);
            root = std::make_unique<Node>();
            root->index = count + FPIndex;
            return;
        }
        root = std::make_unique<Node>();
        auto bw_ten = tenSize;
        if (const auto ten_left = createBSTFor3orMore(bw, bw_ten, index, root, -1 + FPIndex); ten_left != 0)
            throw std::invalid_argument("should be 0\n");
    }

    size_t getOffsetIdx(BitsetWrapper<FINGERPRINT_SIZE>& fp) const {
        return _getOffsetIdx(fp, root);
    }

    size_t getBitRep(const std::unique_ptr<Node>& node, size_t& index, size_t ten_remaining,
                     BitsetWrapper<REGISTER_SIZE>& bw, const int prev_count) {
        if (ten_remaining == 0 || ten_remaining == 1) {
            return ten_remaining;
        }
        if (ten_remaining == 2) {
            setContiguous(node->index - prev_count, index, bw);
            return 0;
        }
        const bool left = node->left != nullptr;
        const bool right = node->right != nullptr;
        bw.set(index++ - startingIndex, !left);
        bw.set(index++ - startingIndex, !right);
        setContiguous(node->index - prev_count, index, bw);
        ten_remaining -= (left ? 0 : 1) + (right ? 0 : 1);
        if (left) {
            ten_remaining = getBitRep(node->left, index, ten_remaining, bw, node->index);
        }
        if (right) {
            ten_remaining = getBitRep(node->right, index, ten_remaining, bw, node->index);
        }
        return ten_remaining;
    }

    void remove(BitsetWrapper<FINGERPRINT_SIZE>& fp) {
        if (tenSize <= 1) {
            throw std::invalid_argument("should only accept ten greater than 1");
        }
        --tenSize;
        if (!root) {
            return;
        }

        Node* current = root.get();
        Node* previous = nullptr;
        Node* preprevious = nullptr;

        while (current) {
            if (fp.get(current->index)) {
                preprevious = previous;
                previous = current;
                current = current->right.get();
            } else {
                preprevious = previous;
                previous = current;
                current = current->left.get();
            }
        }
        if (preprevious == nullptr) {  // root deleted - so previous must be the root
            auto& otherChild = fp.get(root->index) ? root->left : root->right;
            root = std::move(otherChild);
            return;
        }

        auto& prevChildOther = fp.get(previous->index) ? previous->left : previous->right;  // not in fp path | not current but sibling
        auto isRight = fp.get(preprevious->index);
        if (isRight)
            preprevious->right = std::move(prevChildOther);
        else
            preprevious->left = std::move(prevChildOther);
    }

    void insert(BitsetWrapper<FINGERPRINT_SIZE>& new_fp, const size_t firstBit) {
        if (tenSize == 0) {
            throw std::invalid_argument("should only accept ten greater than 0");
        }

        auto newNode = std::make_unique<Node>();
        newNode->index = firstBit;

        if (!root) {
            root = std::move(newNode);
            ++tenSize;
            return;
        }

        Node* current = root.get();
        Node* previous = nullptr;

        while (true) {
            if (current->index > firstBit) {
                if (!previous) {
                    if (new_fp.get(firstBit)) {
                        newNode->left = std::move(root);
                    } else {
                        newNode->right = std::move(root);
                    }
                    root = std::move(newNode);
                } else {
                    auto& child = new_fp.get(previous->index) ? previous->right : previous->left;
                    if (new_fp.get(firstBit)) {
                        newNode->left = std::move(child);
                    } else {
                        newNode->right = std::move(child);
                    }
                    child = std::move(newNode);
                }
                ++tenSize;
                return;
            }

            previous = current;
            current = new_fp.get(current->index) ? current->right.get() : current->left.get();

            if (!current) {
                auto& child = new_fp.get(previous->index) ? previous->right : previous->left;
                child = std::move(newNode);
                ++tenSize;
                return;
            }
        }
    }

    Rep<REGISTER_SIZE> getBitRepWrapper() {
        // debug this func sth is wrong here
        BitsetWrapper<REGISTER_SIZE> bw_res;
        size_t index = startingIndex;
        getBitRep(root, index, tenSize, bw_res, -1 + FPIndex);
        if (tenSize >= 2) {  // Delimiter Bit
            bw_res.set(index++ - startingIndex, true);
        }
        return Rep<REGISTER_SIZE>{bw_res, static_cast<uint32_t>(index - startingIndex)};
    }
};
