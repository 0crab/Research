#include <iostream>
#include <cstring>

using namespace std;



using partial_t = uint8_t;
using size_type = size_t;

static const int partial_offset = 56;

static const uint64_t partial_mask = 0xffull << partial_offset;

static const uint32_t kHashSeed = 7079;

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

inline uint64_t merge_partial(partial_t partial,uint64_t ptr){
    return ((uint64_t)partial << partial_offset) | ptr;
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


#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->buf
#define ITEM_KEY_LEN(item_ptr)  ((Item * )item_ptr)->key_len
#define ITEM_VALUE(item_ptr) (((Item * )item_ptr)->buf + key_len)
#define ITEM_VALUE_LEN(item_ptr)  ((Item * )item_ptr)->value_len


struct Item{
    uint16_t key_len;
    uint16_t value_len;
    char buf[];
};

Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
    cout<<"item len: "<<key_len + value_len+sizeof (uint16_t) + sizeof (uint16_t)<<endl;
    Item * p = (Item * )malloc(key_len + value_len+sizeof (uint16_t) + sizeof (uint16_t));
    memset(p,'#',key_len + value_len+sizeof (uint16_t) + sizeof (uint16_t));
    p->key_len = key_len;
    p->value_len = value_len;
    memcpy(ITEM_KEY(p),key,key_len);
    memcpy(ITEM_VALUE(p),value,value_len);
    return p;
}

int main(){
    char * key = (char *)calloc(1,8 * sizeof (char));
    memset(key,'*',8);
    sprintf(key,"%d",12);

    char * value = (char *)calloc(1,8 * sizeof (char));
    memset(value,'@',8);
    sprintf(value,"%d",34);

    Item * item = allocate_item(key,8,value,8);
    int a=1;

}