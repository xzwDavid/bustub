//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::memset(local_depths_, 0, sizeof(local_depths_));
  std::memset(bucket_page_ids_, 0, sizeof(bucket_page_ids_));

}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (1u << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1u << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;

}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1u << local_depths_[bucket_idx]);
}


auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  global_depth_++;
  for (uint32_t i = 0; i < (1u << (global_depth_ - 1)); i++) {
    bucket_page_ids_[i + (1u << (global_depth_ - 1))] = bucket_page_ids_[i];
    local_depths_[i + (1u << (global_depth_ - 1))] = local_depths_[i];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return global_depth_ > 0 && std::all_of(local_depths_, local_depths_ + Size(), [this](uint8_t ld) { return ld < global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return 1u << global_depth_;
}


auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  return max_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
//  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= Size()) {
    throw std::out_of_range("Bucket index out of range");
  }
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
