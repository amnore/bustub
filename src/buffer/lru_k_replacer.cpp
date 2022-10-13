//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cassert>
#include <deque>
#include <limits>
#include <map>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  assert(frame_id);
  auto lock = std::scoped_lock{latch_};

  if (lru_timestamps_.empty()) {
    return false;
  }

  auto id = lru_timestamps_.begin()->second;
  RemoveInternal(id);
  *frame_id = id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  assert(frame_id < (frame_id_t)replacer_size_);

  auto lock = std::scoped_lock{latch_};
  auto it = frames_.find(frame_id);
  if (it == frames_.end()) {
    it = frames_.emplace(frame_id, FrameStatus{}).first;

    auto ts = std::numeric_limits<timestamp_t>::min() + current_timestamp_;
    it->second.access_timestamps_.push(ts);
  }

  auto &frame = it->second;
  auto ts = current_timestamp_++;

  frame.access_timestamps_.push(ts);
  if (frame.access_timestamps_.size() > k_) {
    auto ts = frame.access_timestamps_.front();
    frame.access_timestamps_.pop();

    if (frame.evictable_) {
      lru_timestamps_.erase(ts);
      lru_timestamps_[frame.access_timestamps_.front()] = frame_id;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto lock = std::scoped_lock{latch_};

  auto &frame = frames_.at(frame_id);
  if (!frame.evictable_ && set_evictable) {
    curr_size_++;
    lru_timestamps_[frame.access_timestamps_.front()] = frame_id;
  } else if (frame.evictable_ && !set_evictable) {
    lru_timestamps_.erase(frame.access_timestamps_.front());
    curr_size_--;
  }
  frame.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto lock = std::scoped_lock{latch_};
  RemoveInternal(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  auto it = frames_.find(frame_id);
  if (it == frames_.end()) {
    return;
  }
  auto &frame = it->second;
  assert(frame.evictable_);

  lru_timestamps_.erase(frame.access_timestamps_.front());
  curr_size_--;
  frames_.erase(it);
}

}  // namespace bustub
