//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_{0}, bucket_size_{bucket_size}, num_buckets_{1}, dir_{1} {
  assert(bucket_size != 0);
  auto initial_bucket = std::make_shared<Bucket>(bucket_size);
  for (auto &ptr : dir_) {
    ptr = initial_bucket;
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  auto lock = std::scoped_lock{latch_};
  auto bucket = dir_[IndexOf(key)];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  auto lock = std::scoped_lock{latch_};
  auto bucket = dir_[IndexOf(key)];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  auto lock = std::scoped_lock{latch_};

  while (true) {
    auto bucket = dir_[IndexOf(key)];

    if (bucket->Insert(key, value)) {
      break;
    }
    RedistributeBucket(bucket);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  assert(bucket->GetItems().size() != 0);

  // allocate a new bucket to hold the elements of which the new bit in hash is 1
  auto depth = bucket->GetDepth();
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
  auto &old_list = bucket->GetItems();
  auto &new_list = new_bucket->GetItems();
  auto hash = std::hash<K>{};
  auto index_bits = hash(old_list.front().first) & ((1 << depth) - 1);
  auto high_bit = 1 << depth;

  for (auto iter = old_list.begin(); iter != old_list.end();) {
    if (hash(iter->first) & high_bit) {
      new_list.emplace_back(std::move(*iter));
      iter = old_list.erase(iter);
    } else {
      iter++;
    }
  }

  if (depth == GetGlobalDepthInternal()) {
    auto size = dir_.size();
    dir_.resize(size * 2);
    std::copy_n(dir_.begin(), size, dir_.begin() + size);
    global_depth_++;
  }
  bucket->IncrementDepth();

  // the indices that points to bucket are index_bits, index_bits+high_bit, ...,
  // and index_bits+high_bit, index_bits+3*high_bit, ... should point to the new bucket
  for (auto i = index_bits + high_bit; i < dir_.size(); i += 2 * high_bit) {
    dir_[i] = new_bucket;
  }
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//

template <typename K>
static auto KeyEquals(const K &key) {
  return [&](const auto &pair) { return pair.first == key; };
}

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto iter = std::find_if(list_.begin(), list_.end(), KeyEquals(key));
  if (iter == list_.end()) {
    return false;
  }

  value = iter->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto iter = std::find_if(list_.begin(), list_.end(), KeyEquals(key));
  if (iter == list_.end()) {
    return false;
  }

  list_.erase(iter);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto iter = std::find_if(list_.begin(), list_.end(), KeyEquals(key));
  if (iter != list_.end()) {
    iter->second = value;
    return true;
  }

  if (!IsFull()) {
    list_.emplace(iter, key, value);
    return true;
  }

  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
