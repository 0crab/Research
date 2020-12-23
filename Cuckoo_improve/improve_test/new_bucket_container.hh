#ifndef BUCKET_CONTAINER_H
#define BUCKET_CONTAINER_H

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

#include "cuckoohash_util.hh"

namespace libcuckoo {

template <std::size_t SLOT_PER_BUCKET>

class bucket_container {
public:
    using size_type =size_t;
public:

  class bucket {
  public:
    bucket() {
        for (int i = 0; i < SLOT_PER_BUCKET; i++) values_[i].store((uint64_t)nullptr);
    }

    //TODO can't define under kick lock is occpupied or not
//    inline bool occupied(size_type ind){return values_[ind].load() != (uint64_t) nullptr;}

  //private:
    friend class bucket_container;

    //TODO align
    std::array<std::atomic<uint64_t>,SLOT_PER_BUCKET> values_;

  };

  bucket_container(size_type hp):hashpower_(hp){
      buckets_ = new bucket[size()]();
  }

  size_type hashpower() const {
    return hashpower_.load(std::memory_order_acquire);
  }

  void hashpower(size_type val) {
    hashpower_.store(val, std::memory_order_release);
  }

  size_type size() const { return size_type(1) << hashpower(); }

  void swap(bucket_container &bc) noexcept {
    size_t bc_hashpower = bc.hashpower();
    hashpower(bc_hashpower);
    std::swap(buckets_, bc.buckets_);
  }


  bucket &operator[](size_type i) { return buckets_[i]; }
  const bucket &operator[](size_type i) const { return buckets_[i]; }

  // ptr has been packaged with partial
  bool try_insertKV(size_type ind, size_type slot, uint64_t insert_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot].load();
        if(old != (uint64_t)nullptr) return false;
        return b.values_[slot].compare_exchange_strong(old,insert_ptr);
  }

  bool try_updateKV(size_type ind, size_type slot,uint64_t old_ptr,uint64_t update_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot].load();
        if(old != old_ptr) return false;
        return b.values_[slot].compare_exchange_strong(old,update_ptr);
  }

  bool try_eraseKV(size_type ind, size_type slot,uint64_t erase_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot].load();
        if(old != erase_ptr) return false;
        return b.values_[slot].compare_exchange_strong(old,(uint64_t) nullptr);
  }

  //only for kick
  //par_ptr holding kick lock
  void set_ptr(size_type ind, size_type slot,uint64_t par_ptr){
      bucket &b = buckets_[ind];
      b.values_[slot].store(par_ptr);
  }

  uint64_t get_item_num(){
      table_mtx.lock();
      uint64_t count = 0;
      for(size_t i = 0; i < size(); i++ ){
           bucket &b = buckets_[i];
           for(int j =0; j< SLOT_PER_BUCKET;j++){
               if(b.values_[j].load() != (uint64_t) nullptr) count++;
           }
      }
      table_mtx.unlock();
      return count;
  }

  bool key_uniqueness(){
      //
  }

private:

  std::atomic<size_type> hashpower_;

  bucket *  buckets_;

  std::mutex table_mtx;
};

}  // namespace libcuckoo

#endif // BUCKET_CONTAINER_H
