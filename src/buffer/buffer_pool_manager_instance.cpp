//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cassert>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  auto lock = std::scoped_lock{latch_};
  auto [page, frame_id] = GetFreePageInternal();
  if (page == nullptr) {
    return nullptr;
  }

  *page_id = page->page_id_ = next_page_id_++;
  page_table_->Insert(*page_id, frame_id);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  auto lock = std::scoped_lock{latch_};
  if (frame_id_t frame_id; page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  auto [page, frame_id] = GetFreePageInternal();
  if (page == nullptr) {
    return nullptr;
  }

  disk_manager_->ReadPage(page_id, page->GetData());
  page->page_id_ = page_id;
  page_table_->Insert(page_id, frame_id);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  auto lock = std::scoped_lock{latch_};
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page->is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  auto lock = std::scoped_lock{latch_};
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  FlushPageInternal(&pages_[frame_id]);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  auto lock = std::scoped_lock{latch_};
  for (size_t id = 0; id < pool_size_; id++) {
    if (pages_[id].GetPageId() != INVALID_PAGE_ID) {
      FlushPageInternal(&pages_[id]);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  auto lock = std::scoped_lock{latch_};

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    return false;
  }

  page_table_->Remove(page->page_id_);
  replacer_->Remove(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetFreePageInternal() -> std::pair<Page *, frame_id_t> {
  Page *page = nullptr;
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else {
    auto can_evict = replacer_->Evict(&frame_id);
    if (!can_evict) {
      return {nullptr, 0};
    }

    page = &pages_[frame_id];
    page_table_->Remove(page->page_id_);
    FlushPageInternal(page);
  }

  replacer_->RecordAccess(frame_id);
  assert(page->GetPinCount() == 0 && !page->IsDirty());
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 1;
  return {page, frame_id};
}

void BufferPoolManagerInstance::FlushPageInternal(Page *page) {
  if (!page->IsDirty()) {
    return;
  }

  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  page->is_dirty_ = false;
}
}  // namespace bustub
