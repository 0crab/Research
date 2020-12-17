
#include <cstring>
#include "new_bucket_container.hh"
#include "cuckoohash_config.hh"
#include "cuckoohash_util.hh"

#include "item.h"

namespace libcuckoo{

static const std::size_t SLOT_PER_BUCKET = DEFAULT_SLOT_PER_BUCKET;

static const int partial_offset = 56;
static const uint64_t partial_mask = 0xffull << partial_offset;
static const uint64_t ptr_mask = ~0ull ^ partial_mask;

class new_cuckoohash_map {
private:
    static const uint32_t kHashSeed = 7079;

    using partial_t = uint8_t;

    using buckets_t = bucket_container<SLOT_PER_BUCKET>;

    using bucket = typename buckets_t::bucket;

    static uint64_t MurmurHash64A(const void *key, size_t len) {
        const uint64_t m = 0xc6a4a7935bd1e995ull;
        const size_t r = 47;
        uint64_t seed = kHashSeed;

        uint64_t h = seed ^(len * m);

        const auto *data = (const uint64_t *) key;
        const uint64_t *end = data + (len / 8);

        while (data != end) {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const auto *data2 = (const unsigned char *) data;

        switch (len & 7ull) {
            case 7:
                h ^= uint64_t(data2[6]) << 48ull;
            case 6:
                h ^= uint64_t(data2[5]) << 40ull;
            case 5:
                h ^= uint64_t(data2[4]) << 32ull;
            case 4:
                h ^= uint64_t(data2[3]) << 24ull;
            case 3:
                h ^= uint64_t(data2[2]) << 16ull;
            case 2:
                h ^= uint64_t(data2[1]) << 8ull;
            case 1:
                h ^= uint64_t(data2[0]);
                h *= m;
        };

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

    struct str_equal_to {
        bool operator()(const char * first, size_t first_len, const char* second, size_t second_len) {
            if(first_len != second_len) return false;
            return !std::memcmp(first,second,first_len);
        }
    };

    struct str_hash {
        std::size_t operator()(const char* str, size_t n) const noexcept {
            return MurmurHash64A((void *)str, n);
        }
    };

public:
    using size_type = typename buckets_t::size_type;

    struct hash_value {
        size_type hash;
        partial_t partial;
    };

    enum cuckoo_status {
        ok,
        failure,
        failure_key_not_found,
        failure_key_duplicated,
        failure_table_full,
        failure_under_expansion,
    };

    struct table_position {
        size_type index;
        size_type slot;
        cuckoo_status status;
    };

    static constexpr uint16_t slot_per_bucket() { return SLOT_PER_BUCKET; }

    size_type hashpower() const { return buckets_.hashpower(); }

    static inline size_type hashmask(const size_type hp) {
        return hashsize(hp) - 1;
    }

    static inline size_type hashsize(const size_type hp) {
        return size_type(1) << hp;
    }

    new_cuckoohash_map(size_type n = DEFAULT_SIZE):buckets_(reserve_calc(n)){
            ;
    }

    static size_type reserve_calc(const size_type n) {
        const size_type buckets = (n + slot_per_bucket() - 1) / slot_per_bucket();
        size_type blog2;
        for (blog2 = 0; (size_type(1) << blog2) < buckets; ++blog2)
            ;
        assert(n <= buckets * slot_per_bucket() && buckets <= hashsize(blog2));
        return blog2;
    }


    static partial_t partial_key(const size_type hash) {
        const uint64_t hash_64bit = hash;
        const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                     static_cast<uint32_t>(hash_64bit >> 32));
        const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                     static_cast<uint16_t>(hash_32bit >> 16));
        const uint8_t hash_8bit = (static_cast<uint8_t>(hash_16bit) ^
                                   static_cast<uint8_t>(hash_16bit >> 8));
        return hash_8bit;
    }


    static inline size_type index_hash(const size_type hp, const size_type hv) {
        return hv & hashmask(hp);
    }

    static inline size_type alt_index(const size_type hp, const partial_t partial,
                                      const size_type index) {
        // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
        // hash constant from 64-bit MurmurHash2
        const size_type nonzero_tag = static_cast<size_type>(partial) + 1;
        return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
    }

    hash_value hashed_key(const char * key,size_type key_len) const {
        const size_type hash = str_hash()(key,key_len);
        return {hash, partial_key(hash)};
    }

    class TwoBuckets {
    public:
        TwoBuckets() {}
        TwoBuckets(size_type i1_, size_type i2_)
                : i1(i1_), i2(i2_) {}

        size_type i1, i2;

    };

    TwoBuckets get_two_buckets(const hash_value &hv) const {
       // while (true) {
            const size_type hp = hashpower();
            const size_type i1 = index_hash(hp, hv.hash);
            const size_type i2 = alt_index(hp, hv.partial, i1);
            //maybe rehash here
            return TwoBuckets(i1, i2);
        //}
    }

    inline partial_t get_partial(size_type par_ptr) const {
        return static_cast<partial_t>((par_ptr & partial_mask)>>partial_offset);
    }

    inline uint64_t get_ptr(size_type par_ptr) const{
        return par_ptr & ptr_mask;
    }

    inline uint64_t merge_partial(partial_t partial,uint64_t ptr){
        return ((uint64_t)partial << partial_offset) | ptr;
    }

    int try_read_from_bucket(const bucket &b, const partial_t partial,
                             const char * key,size_type key_len) const {

        for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
            size_type par_ptr = b.values_[i].load();
            partial_t read_partial = get_partial(par_ptr);
            uint64_t read_ptr = get_ptr(par_ptr);
            if (read_ptr == (size_type)nullptr ||
                            ( partial != read_partial) ){
                continue;
            } else if (str_equal_to()(ITEM_KEY(read_ptr),ITEM_KEY_LEN(read_ptr),key,key_len)) {
                return i;
            }
        }
        return -1;
    }

    table_position cuckoo_find(const char * key,size_type key_len, const partial_t partial,
                               const size_type i1, const size_type i2) const {
        int slot = try_read_from_bucket(buckets_[i1], partial, key,key_len);
        if (slot != -1) {
            return table_position{i1, static_cast<size_type>(slot), ok};
        }
        slot = try_read_from_bucket(buckets_[i2], partial, key,key_len);
        if (slot != -1) {
            return table_position{i2, static_cast<size_type>(slot), ok};
        }
        return table_position{0, 0, failure_key_not_found};
    }

    //false : key_duplicated. the slot is the position of deplicated key
    //true : have empty slot.the slot is the position of empty slot. -1 -> no empty slot
    bool try_find_insert_bucket(const bucket &b, int &slot,
                                const partial_t partial, const char *key,size_type key_len) const {
        // Silence a warning from MSVC about partial being unused if is_simple.
        //(void)partial;
        slot = -1;
        for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
            size_type par_ptr = b.values_[i].load();
            partial_t read_partial = get_partial(par_ptr);
            uint64_t read_ptr = get_ptr(par_ptr);
            if (read_ptr != (size_type) nullptr) {
                if (partial != read_partial) {
                    continue;
                }
                if (str_equal_to()(ITEM_KEY(read_ptr),ITEM_KEY_LEN(read_ptr),key,key_len)) {
                    slot = i;
                    return false;
                }
            } else {
                slot = i;
            }
        }
        return true;
    }

    table_position cuckoo_insert(const hash_value hv, TwoBuckets &b,char * key,size_type key_len) {
        int res1, res2;
        bucket &b1 = buckets_[b.i1];
        if (!try_find_insert_bucket(b1, res1, hv.partial, key,key_len)) {
            return table_position{b.i1, static_cast<size_type>(res1),
                                  failure_key_duplicated};
        }
        bucket &b2 = buckets_[b.i2];
        if (!try_find_insert_bucket(b2, res2, hv.partial, key,key_len)) {
            return table_position{b.i2, static_cast<size_type>(res2),
                                  failure_key_duplicated};
        }
        if (res1 != -1) {
            return table_position{b.i1, static_cast<size_type>(res1), ok};
        }
        if (res2 != -1) {
            return table_position{b.i2, static_cast<size_type>(res2), ok};
        }
        std::cout<<"bucket full, need kick"<<std::endl;
        assert(false);
    }

    table_position cuckoo_insert_loop(hash_value hv, TwoBuckets &b, char * key, size_type key_len) {
        table_position pos;
        while (true) {
            const size_type hp = hashpower();
            pos = cuckoo_insert(hv, b, key,key_len);
            switch (pos.status) {
                case ok:
                case failure_key_duplicated:
                    return pos;
                case failure_table_full:
                    assert(false);
                    // Expand the table and try again, re-grabbing the locks
                    //cuckoo_fast_double<TABLE_MODE, automatic_resize>(hp);
                    //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                    break;
                case failure_under_expansion:
                    assert(false);
                    // The table was under expansion while we were cuckooing. Re-grab the
                    // locks and try again.
                    //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                    break;
                default:
                    assert(false);
            }
        }
    }

    inline bool check_ptr(uint64_t ptr,char * key,size_type key_len){
        if(ptr == (uint64_t)nullptr) return false;
        return str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),key,key_len);
    }

    uint64_t get_item_num(){return buckets_.get_item_num();}

    //true hit , false miss
    bool find(char * key,size_t len);
    //true insert , false key failure_key_duplicated
    bool insert(char * key,size_t key_len,char * value ,size_t value_len);

    bool insert_or_assign(char * key,size_t key_len,char * value ,size_t value_len);
    //true erase success, false miss
    bool erase(char * key, size_t key_len);


    mutable buckets_t buckets_;

};

bool new_cuckoohash_map::find(char* key,size_t key_len){
    const hash_value hv = hashed_key(key,key_len);
    TwoBuckets b = get_two_buckets(hv);
    table_position pos = cuckoo_find(key,key_len,hv.partial,b.i1,b.i2);
    if(pos.status == ok){
        //do some thing
        return true;
    }
    return false;
}

bool new_cuckoohash_map::insert(char *key, size_t key_len, char *value, size_t value_len) {
    Item  * item = allocate_item(key,key_len,value,value_len);
    const hash_value hv = hashed_key(key,key_len);
    while(true){
        TwoBuckets b = get_two_buckets(hv);
        table_position pos = cuckoo_insert_loop(hv, b, key,key_len);
        if (pos.status == ok) {
            if(buckets_.try_insertKV(pos.index,pos.slot,merge_partial(hv.partial,(uint64_t)item))){
                return true;
            }
        }else{
            //key_duplicated
            return false;
        }
    }
}

bool new_cuckoohash_map::insert_or_assign(char *key, size_t key_len, char *value, size_t value_len) {
    Item  * item = allocate_item(key,key_len,value,value_len);
    const hash_value hv = hashed_key(key,key_len);
    while(true){
        TwoBuckets b = get_two_buckets(hv);
        table_position pos = cuckoo_insert_loop(hv, b, key,key_len);
        if (pos.status == ok) {
            if(buckets_.try_insertKV(pos.index,pos.slot,merge_partial(hv.partial,(uint64_t)item))){
                return true;
            }
        }else{
            uint64_t par_ptr = buckets_[pos.index].values_[pos.slot].load();
            uint64_t update_ptr = get_ptr(par_ptr);
            if(check_ptr(update_ptr,key,key_len)){
                if(buckets_.try_updateKV(pos.index,pos.slot,par_ptr)){
                    return false;
                }
            }
        }
    }
}

bool new_cuckoohash_map::erase(char *key, size_t key_len) {
    const hash_value hv = hashed_key(key,key_len);
    while(true){
        TwoBuckets b = get_two_buckets(hv);
        table_position pos = cuckoo_find(key,key_len,hv.partial,b.i1,b.i2);
        if(pos.status == ok){
            uint64_t par_ptr = buckets_[pos.index].values_[pos.slot].load();
            uint64_t erase_ptr = get_ptr(par_ptr);
            if(check_ptr(erase_ptr,key,key_len)){
                if(buckets_.try_eraseKV(pos.index,pos.slot,par_ptr)){
                    return true;
                }
            }
        }else{
            //return false only when key not find
            return false;
        }
    }
}

}
