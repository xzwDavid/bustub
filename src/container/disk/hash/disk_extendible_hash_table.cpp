//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
//  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
    index_name_ = name;
    BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    header_page->Init(1);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash_value = Hash(key);
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  // Assume DirectoryIndex() calculates the directory index from the hash value and current global depth.
  auto dir_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value));

  // Fetch the directory page to find the bucket page id.
  auto directory_page = bpm_->FetchPageBasic(dir_page_id); // Simplified method call
  auto directory = directory_page.AsMut< ExtendibleHTableDirectoryPage >();

  auto bucket_idx = directory->HashToBucketIndex(hash_value);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  auto bucket_page = bpm_->FetchPageBasic(bucket_page_id); // Simplified method call
  auto bucket = bucket_page.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();

  if (!bucket->IsFull()) {
    // Insert directly if there is space.
    bucket->Insert(key, value, cmp_);
    bpm_->UnpinPage(bucket_page_id, true); // Unpin with changes
  } else {
    // Handle bucket overflow and potentially split.

    if (!InsertToNewBucket(directory,bucket_idx,key,value)) {
      // If SplitBucket fails, it means we couldn't insert for some reason.
      if()
    }
  }

  // Simplified; real implementation should handle exceptions and edge cases.
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  uint32_t global_depth = directory->GetGlobalDepth();

  if (local_depth < global_depth) {
    // 有分裂桶或加倍目录的空间
    uint32_t new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    page_id_t new_bucket_page_id;
    BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    // 重新分配条目并插入新的键值对
    if (SplitBucketAndRedistribute(directory, bucket_idx, new_bucket_idx)) {
      // 成功分裂和重新分配
      directory->IncrLocalDepth(bucket_idx);
      if (local_depth == global_depth) {
        // 需要加倍目录大小
        directory->IncrGlobalDepth();
        // 更新全局深度后的映射
        UpdateDirectoryMapping(directory, bucket_idx, new_bucket_idx);
      }

      directory->SetLocalDepth(new_bucket_idx, directory->GetLocalDepth(bucket_idx));

      // 确保按需锁定/解锁页面
      bpm_->UnpinPage(new_bucket_page_id, true); // 标记新桶页面为脏页
      return true;
    }else if(local_depth == global_depth && global_depth < directory_max_depth_){

    }else {
      // 如果重新分配失败，回滚任何更改
      bpm_->UnpinPage(new_bucket_page_id, false); // 无需将新桶页面标记为脏页
      return false;
    }
  } else {
    // 由于目录深度限制，无法进一步分裂桶
    return false;
  }

}

template <typename K, typename V, typename KC>
bool DiskExtendibleHashTable<K, V, KC>::SplitBucketAndRedistribute(
    ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx, uint32_t new_bucket_idx) {
  // 获取旧桶页面ID和页面本身
  page_id_t old_bucket_page_id = directory->GetBucketPageId(bucket_idx);
  BasicPageGuard old_bucket_guard = bpm_->FetchPageBasic(old_bucket_page_id);
  auto old_bucket = old_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 获取新桶页面ID和页面本身，假设已在调用此函数前初始化
  page_id_t new_bucket_page_id = directory->GetBucketPageId(new_bucket_idx);
  BasicPageGuard new_bucket_guard = bpm_->FetchPageBasic(new_bucket_page_id);
  auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();



  // 实际代码中，这里还需要处理一些细节，比如错误处理和回滚机制
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
