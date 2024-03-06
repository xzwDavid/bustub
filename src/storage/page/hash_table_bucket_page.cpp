//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool found = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsOccupied(i) && IsReadable(i)) {
      if (cmp(KeyAt(i), key) == 0) {
        result->push_back(ValueAt(i));
        found = true;
      }
    }
  }
  return found;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsOccupied(i) && IsReadable(i)) {
      // 如果键相等，则判断为重复，不允许插入
      if (cmp(KeyAt(i), key) == 0 && ValueAt(i) == value) {
        return false;
      }
    } else if (!IsOccupied(i)) {
      // 找到一个未被占用的槽位，进行插入
      array_[i] = std::make_pair(key, value);
      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }
  // 如果桶已满，则返回false
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsOccupied(i) && IsReadable(i)) {
      // 如果键值对匹配，则进行移除
      if (cmp(KeyAt(i), key) == 0 && ValueAt(i) == value) {
        SetUnReadable(i); // 标记为不可读，即移除
        SetUnOccupied(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (IsOccupied(bucket_idx) && IsReadable(bucket_idx)) {
    // 设置指定位置为不可读，表示移除键值对，但保留占用标志以保持桶的结构完整
    readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  return occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  return readable_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t count = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      ++count;
    }
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsOccupied(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
