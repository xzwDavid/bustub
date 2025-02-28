#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop(); // Ensure the current guard drops its resources.
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard(){
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : guard_(std::move(that.guard_)) {
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) { // 1. Check for self-assignment
    this->Drop(); // 2. Release any resources currently held
    guard_ = std::move(that.guard_); // 3. Take ownership of the resources from 'that'
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.page_ != nullptr){
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {Drop();}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept: guard_(std::move(that.guard_)){}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // Unlatch the page for write
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  auto readPageGuard =  ReadPageGuard(this->bpm_, this->page_);
  this->Drop();
  return readPageGuard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  auto writePageGuard =  WritePageGuard(this->bpm_, this->page_);
  this->Drop();
  return writePageGuard ;
}



}  // namespace bustub
