//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  // Initialize all directory page ids to an invalid value indicating they are unused.
  std::memset(directory_page_ids_, -1, sizeof(directory_page_ids_));
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  // This function assumes the hash function will produce a 32-bit hash.
  // We use the first 'max_depth_' bits of the hash to determine the directory index.
  return hash & ((1 << max_depth_) - 1);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  if (directory_idx >= (1 << max_depth_)) {
    throw std::out_of_range("Directory index out of range");
  }
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  if (directory_idx >= (1 << max_depth_)) {
    throw std::out_of_range("Directory index out of range");
  }
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return 1 << max_depth_;
}

void ExtendibleHTableHeaderPage::PrintHeader() const {
  // Simple iteration over the directory and printing its content
  std::cout << "Extendable Hash Table Header Page:" << std::endl;
  std::cout << "Max Depth: " << max_depth_ << std::endl;
  std::cout << "Directory Page IDs:" << std::endl;
  for (size_t i = 0; i < (1 << max_depth_); ++i) {
    std::cout << "Index " << i << ": Page ID " << directory_page_ids_[i] << std::endl;
  }
}

}  // namespace bustub
