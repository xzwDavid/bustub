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
    header_page->Init();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
    uint32_t hash_value = Hash(key);
    BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    auto dir_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value));

    BasicPageGuard directory_guard = bpm_->FetchPageBasic(dir_page_id);
    auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

    auto bucket_idx = directory->HashToBucketIndex(hash_value);
    auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

    BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
    auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    V result_v;
    bool found = bucket->Lookup(key, result_v, cmp_);
    result->push_back(result_v);
    bpm_->UnpinPage(bucket_page_id, false); // Unpin the page without changes

    return found;
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
  auto dir_idx = header_page->HashToDirectoryIndex(hash_value);
  auto dir_page_id = header_page->GetDirectoryPageId(dir_idx);
  if((int)dir_page_id == -1){
    return InsertToNewDirectory(header_page, dir_idx, hash_value, key, value);
  }

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
    return InsertToNewBucket(directory,bucket_idx,key,value);
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  if( directory_idx > header->MaxSize()){
    return false;
  }

  auto dir_page_id = INVALID_PAGE_ID;
  auto dir_page_guard = bpm_->NewPageGuarded(&dir_page_id);
  auto dir_page = dir_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  header->SetDirectoryPageId(directory_idx, dir_page_id);
  auto bucket_idx = dir_page->HashToBucketIndex(hash);
  return InsertToNewBucket(dir_page,bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  uint32_t global_depth = directory->GetGlobalDepth();

  auto old_bucket_guard = bpm_->FetchPageBasic(directory->GetBucketPageId(bucket_idx));
  auto old_bucket = old_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (local_depth < global_depth || (local_depth == global_depth && global_depth < directory_max_depth_)) {
    // 有分裂桶或加倍目录的空间
    uint32_t new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    page_id_t new_bucket_page_id;
    BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);

    directory->IncrLocalDepth(bucket_idx);
    UpdateDirectoryMapping(directory, new_bucket_idx, new_bucket_page_id, local_depth+1, bucket_idx & ((1u << local_depth) - 1));


    std::vector<std::pair<K,V>> all_key_values;
    all_key_values.push_back(std::make_pair(key, value));
    for (int i = 0; i < (int)old_bucket->Size(); i++) {
      all_key_values.push_back(std::make_pair(old_bucket->KeyAt(i), old_bucket->ValueAt(i)));
      old_bucket->RemoveAt(i); // remove干净
    }
    bool insert_success;
    for (size_t i = 0; i < all_key_values.size(); i++) {
      // ! 靠, 之前这里 key 传递成了上面的key, 导致 hash值一直是一样的我日
      insert_success  = Insert(key,value, nullptr);
    }

    if (insert_success) {
      // 成功分裂和重新分配
      // 确保按需锁定/解锁页面
      bpm_->UnpinPage(new_bucket_page_id, true); // 标记新桶页面为脏页
      return true;
    }else {
      // 如果重新分配失败，回滚任何更改
      std::cout << "SplitInsert 失败: " << value << std::endl;
      bpm_->UnpinPage(new_bucket_page_id, false); // 无需将新桶页面标记为脏页
      return false;
    }
  } else {
    // 由于目录深度限制，无法进一步分裂桶
    return false;
  }
}



template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
//  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);


  if (new_local_depth > directory->GetGlobalDepth()) {
    directory->IncrGlobalDepth();
  }
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);

  auto mask = (1u << (new_local_depth - 1))  - 1;
  for(auto i = 0; i < (int)directory->Size() ; i++){
    auto masked_value = (i & mask) | (1u << (new_local_depth  - 1));
    if((masked_value ^ local_depth_mask) == 0){
      directory->SetBucketPageId(i,new_bucket_page_id);
    }
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash_value = Hash(key);
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto dir_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash_value));

  BasicPageGuard directory_guard = bpm_->FetchPageBasic(dir_page_id);
  auto directory = directory_guard.As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash_value);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  bool removed = bucket->Remove(key, cmp_);
  bpm_->UnpinPage(bucket_page_id, removed); // Unpin the page, mark as dirty if removed

  return removed;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
