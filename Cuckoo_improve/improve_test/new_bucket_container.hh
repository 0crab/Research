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

template <class Key, class T, class Allocator,std::size_t SLOT_PER_BUCKET>
class bucket_container {
public:
  using key_type = Key;
  using mapped_type = T;
  using value_type = std::pair<const Key, T>;

private:
  using traits_ = typename std::allocator_traits<Allocator>::template rebind_traits<value_type>;

public:
  using allocator_type = typename traits_::allocator_type;
  //using partial_t = Partial;
  using size_type = typename traits_::size_type;
  using reference = value_type &;
  using const_reference = const value_type &;
  using pointer = typename traits_::pointer;
  using const_pointer = typename traits_::const_pointer;

  /*
   * The bucket type holds SLOT_PER_BUCKET key-value pairs, along with their
   * partial keys and occupancy info. It uses aligned_storage arrays to store
   * the keys and values to allow constructing and destroying key-value pairs
   * in place. The lifetime of bucket data should be managed by the container.
   * It is the user's responsibility to confirm whether the data they are
   * accessing is live or not.
   */
  class bucket {
  public:
    bucket() {
        for(int i = 0;i < SLOT_PER_BUCKET;i++) values_[i].store(nullptr);
    }


  private:
    friend class bucket_container;

    using storage_value_type = std::pair<Key, T>;

    //TODO align
    std::array<std::atomic<uint64_t>,SLOT_PER_BUCKET> values_;

  };

  bucket_container(size_type hp, const allocator_type &allocator)
      : allocator_(allocator), bucket_allocator_(allocator), hashpower_(hp),
        buckets_(bucket_allocator_.allocate(size())) {

    static_assert(std::is_nothrow_constructible<bucket>::value,
                  "bucket_container requires bucket to be nothrow "
                  "constructible");
    for (size_type i = 0; i < size(); ++i) {
      traits_::construct(allocator_, &buckets_[i]);
    }
  }

  bucket_container(const bucket_container &bc)
      : allocator_(
            traits_::select_on_container_copy_construction(bc.allocator_)),
        bucket_allocator_(allocator_), hashpower_(bc.hashpower()),
        buckets_(transfer(bc.hashpower(), bc, std::false_type())) {}

  bucket_container(const bucket_container &bc,
                             const allocator_type &a)
      : allocator_(a), bucket_allocator_(allocator_),
        hashpower_(bc.hashpower()),
        buckets_(transfer(bc.hashpower(), bc, std::false_type())) {}

  bucket_container(bucket_container &&bc)
      : allocator_(std::move(bc.allocator_)), bucket_allocator_(allocator_),
        hashpower_(bc.hashpower()), buckets_(std::move(bc.buckets_)) {
    // De-activate the other buckets container
    bc.buckets_ = nullptr;
  }

  bucket_container(bucket_container &&bc,
                             const allocator_type &a)
      : allocator_(a), bucket_allocator_(allocator_) {
    move_assign(bc, std::false_type());
  }


  size_type hashpower() const {
    return hashpower_.load(std::memory_order_acquire);
  }

  void hashpower(size_type val) {
    hashpower_.store(val, std::memory_order_release);
  }

  size_type size() const { return size_type(1) << hashpower(); }

  allocator_type get_allocator() const { return allocator_; }

  bucket &operator[](size_type i) { return buckets_[i]; }
  const bucket &operator[](size_type i) const { return buckets_[i]; }

  // ptr has been packaged with partial
  bool try_insertKV(size_type ind, size_type slot, uint64_t insert_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot].load();
        if(old != (uint64_t)nullptr) return false;
        return b.values_[slot].compare_exchange_strong(old,insert_ptr);
  }

  bool try_updateKV(size_type ind, size_type slot, uint64_t update_ptr) {
      bucket &b = buckets_[ind];
      uint64_t old = b.values_[slot].load();
      return b.values_[slot].compare_exchange_strong(old,update_ptr);
  }

  bool try_eraseKV(size_type ind, size_type slot) {
        bucket &b = buckets_[ind];
      uint64_t old = b.values_[slot].load();
      return b.values_[slot].compare_exchange_strong(old,(uint64_t) nullptr);
  }


private:
  using bucket_traits_ = typename traits_::template rebind_traits<bucket>;
  using bucket_pointer = typename bucket_traits_::pointer;

  // true here means the allocators from `src` are propagated on libcuckoo_copy
  template <typename A>
  void copy_allocator(A &dst, const A &src, std::true_type) {
    dst = src;
  }

  template <typename A>
  void copy_allocator(A &dst, const A &src, std::false_type) {}

  // true here means the allocators from `src` are propagated on libcuckoo_swap
  template <typename A> void swap_allocator(A &dst, A &src, std::true_type) {
    std::swap(dst, src);
  }

  template <typename A> void swap_allocator(A &, A &, std::false_type) {}

  // true here means the bucket allocator should be propagated
  void move_assign(bucket_container &src, std::true_type) {
    allocator_ = std::move(src.allocator_);
    bucket_allocator_ = allocator_;
    hashpower(src.hashpower());
    buckets_ = src.buckets_;
    src.buckets_ = nullptr;
  }

  void move_assign(bucket_container &src, std::false_type) {
    hashpower(src.hashpower());
    if (allocator_ == src.allocator_) {
      buckets_ = src.buckets_;
      src.buckets_ = nullptr;
    } else {
      buckets_ = transfer(src.hashpower(), src, std::true_type());
    }
  }


  // This allocator matches the value_type, but is not used to construct
  // storage_value_type pairs, or allocate buckets
  allocator_type allocator_;
  // This allocator is used for actually allocating buckets. It is simply
  // copy-constructed from `allocator_`, and will always be copied whenever
  // allocator_ is copied.
  typename traits_::template rebind_alloc<bucket> bucket_allocator_;
  // This needs to be atomic, since it can be read and written by multiple
  // threads not necessarily synchronized by a lock.
  std::atomic<size_type> hashpower_;
  // These buckets are protected by striped locks (external to the
  // BucketContainer), which must be obtained before accessing a bucket.
  bucket_pointer buckets_;

};

}  // namespace libcuckoo

#endif // BUCKET_CONTAINER_H
