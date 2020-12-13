#include <iostream>
#include <random>
#include <vector>
#include <mutex>
#include <atomic>
#include "tracer.h"
#include "item.h"
#include "new_bucket_container.hh"

using namespace libcuckoo;

using Key = uint64_t;
using Value = uint64_t;
using partial_t = uint8_t;

inline bool key_equal(const Key & k1, const Key & k2){
    return k1 == k2;
}

using buckets_t =
bucket_container<Key, Value , std::allocator<std::pair<const Key, Value>>, partial_t, DEFAULT_SLOT_PER_BUCKET>;
std::allocator<std::pair<const Key, Value>> alloc;
buckets_t buckets_(1, alloc);
using bucket = typename buckets_t::bucket;

class simple_lock{
public:
    simple_lock(){lock_.clear();}
    std::atomic_flag lock_;
    void lock(){ while (lock_.test_and_set(std::memory_order_acq_rel));}
    void unlock() noexcept { lock_.clear(std::memory_order_release); }
};
std::vector<simple_lock> locks(2);

static const int op_type_num = 5;
enum Op_type{
    Find = 0,
    Insert = 1,
    Set = 2, // Insert_or_Assign
    Update = 3,
    Erase = 4
};

static const int bucket_type_num = 3;
enum Bucket_type{
    FIRST_BUCKET = 0,
    SECOND_BUCKET = 1,
    BOTH_BUCKET = 2
};


typedef struct Request{
    Op_type optype;
    Bucket_type index;
    Key key;
    Value value;
};

static const size_t VALUE_DEFAULT = 12345678987654321;

Request * requests;

int thread_num = 1;
int key_range = 1;
int total_count = 1;
int timer_range = 0;

static size_t find_success,find_failure;
static size_t insert_success,insert_failure_duplicated,insert_failure_need_kick;
static size_t set_success,set_failure;
static size_t update_success,update_failure;
static size_t erase_success,erase_failure;

thread_local static size_t find_success_l,find_failure_l;
thread_local static size_t insert_success_l,insert_failure_duplicated_l,insert_failure_need_kick_l;
thread_local static size_t set_success_l,set_failure_l;
thread_local static size_t update_success_l,update_failure_l;
thread_local static size_t erase_success_l,erase_failure_l;


uint64_t * runtimelist;
uint64_t op_num;

std::atomic<int> stopMeasure(0);

enum cuckoo_status {
    ok,
    failure,
    failure_key_not_found,
    failure_key_duplicated,
    failure_table_full,
    failure_under_expansion,
    need_kick
};
struct table_position {
    size_t index;
    size_t slot;
    cuckoo_status status;
};

inline void merge_log(){
    __sync_fetch_and_add(&find_success,find_success_l);
    __sync_fetch_and_add(&find_failure,find_failure_l);
    __sync_fetch_and_add(&insert_success,insert_success_l);
    __sync_fetch_and_add(&insert_failure_duplicated,insert_failure_duplicated_l);
    __sync_fetch_and_add(&insert_failure_need_kick,insert_failure_need_kick_l);
    __sync_fetch_and_add(&set_success,set_success_l);
    __sync_fetch_and_add(&set_failure,set_failure_l);
    __sync_fetch_and_add(&update_success,update_success_l);
    __sync_fetch_and_add(&update_failure,update_failure_l);
    __sync_fetch_and_add(&erase_success,erase_success_l);
    __sync_fetch_and_add(&erase_failure,erase_failure_l);
}

void show_info(){
    std::cout<<" find_success "<<find_success<<" find_failure "<<find_failure<<std::endl;
    std::cout<<" insert_success "<<insert_success<<" insert_failure_duplicated "<<insert_failure_duplicated<<
                                                    " insert_failure_need_kick "<<insert_failure_need_kick<<std::endl;
    std::cout<<" set_success "<<set_success<<" set_failure "<<set_failure<<std::endl;
    std::cout<<" update_success "<<update_success<<" update_failure "<<update_failure<<std::endl;
    std::cout<<" erase_success "<<erase_success<<" erase_failure "<<erase_failure<<std::endl;

    std::cout<<" op_num "<<op_num<<std::endl;

    uint64_t runtime = 0 ;
    for(int i = 0; i < thread_num; i++){
        runtime += runtimelist[i];
    }
    runtime /= thread_num;
    std::cout<<"***runtime "<<runtime<<std::endl;

}


int try_read_from_bucket(const bucket &b, const partial_t partial,
                         const Key &key) {
    // Silence a warning from MSVC about partial being unused if is_simple.
    (void)partial;
    for (int i = 0; i < 4; ++i) {
        if (!b.occupied(i) || ( partial != b.partial(i))) {
            continue;
        } else if (key_equal(b.key(i), key)) {
            return i;
        }
    }
    return -1;
}

inline int find_func(const Request & req){
    int ret = -1;
    partial_t partial = (partial_t) req.key;
    if(req.index != BOTH_BUCKET){
        ret = try_read_from_bucket(buckets_[req.index],partial,req.key);
    }else{
        ret = try_read_from_bucket(buckets_[FIRST_BUCKET],partial,req.key);
        if(ret == - 1) ret = try_read_from_bucket(buckets_[SECOND_BUCKET],partial,req.key);
    }
    return ret;
}

bool try_find_insert_bucket(const bucket &b, int &slot,
                            const partial_t partial, const Key &key) {
    // Silence a warning from MSVC about partial being unused if is_simple.
    (void)partial;
    slot = -1;
    for (int i = 0; i < 4; ++i) {
        if (b.occupied(i)) {
            if (partial != b.partial(i)) {
                continue;
            }
            if (key_equal(b.key(i), key)) {
                slot = i;
                return false;
            }
        } else {
            slot = i;
        }
    }
    return true;
}


table_position insert_func(const Request & req){
    partial_t partial = (partial_t) req.key;
    int res1, res2;
    if(req.index != BOTH_BUCKET){
        if (!try_find_insert_bucket(buckets_[req.index], res1, partial, req.key)) {
            return table_position{req.index, static_cast<size_t>(res1),
                                  failure_key_duplicated};
        }
        if (res1 != -1) {
            return table_position{req.index, static_cast<size_t>(res1), ok};
        }
        return table_position{req.index,0,need_kick};
    }else{
        if (!try_find_insert_bucket(buckets_[FIRST_BUCKET], res1, partial, req.key)) {
            return table_position{FIRST_BUCKET, static_cast<size_t>(res1),
                                  failure_key_duplicated};
        }
        if (!try_find_insert_bucket(buckets_[SECOND_BUCKET], res2, partial, req.key)) {
            return table_position{SECOND_BUCKET, static_cast<size_t>(res2),
                                  failure_key_duplicated};
        }
        if (res1 != -1) {
            return table_position{FIRST_BUCKET, static_cast<size_t>(res1), ok};
        }
        if (res2 != -1) {
            return table_position{SECOND_BUCKET, static_cast<size_t>(res2), ok};
        }
        return table_position{FIRST_BUCKET,0,need_kick};
    }
}

void op_func(const Request & req){
    //lock before operate
    if(req.index == BOTH_BUCKET){
        locks[0].lock();
        locks[1].lock();
    }else{
        locks[req.index].lock();
    }
    switch (req.optype) {
        case Find :{
                int ret = find_func(req);
                if(ret != -1)
                    find_success_l++;
                else
                    find_failure_l++;
            }
            break;
        case Insert :{
                partial_t partial = (partial_t) req.key;
                table_position pos = insert_func(req);
                if(pos.status == ok){
                    buckets_.setKV(pos.index,pos.slot,partial,req.key,req.value);
                    insert_success_l++;
                }else if (pos.status == failure_key_duplicated){
                    insert_failure_duplicated_l++;
                }else{
                    insert_failure_need_kick_l++;
                }
            }
            break;
        case Set :{
                partial_t partial = (partial_t) req.key;
                table_position pos = insert_func(req);
                if(pos.status == ok){
                    buckets_.setKV(pos.index,pos.slot,partial,req.key,req.value);
                    set_success_l++;
                }else if (pos.status == failure_key_duplicated){
                    buckets_[pos.index].mapped(pos.slot) = req.value;
                    set_success_l++;
                }else{
                    set_failure_l++;
                }
            }
            break;
        case Update :{
                partial_t partial = (partial_t) req.key;
                table_position pos = insert_func(req);
                if(pos.status == failure_key_duplicated){
                    buckets_[pos.index].mapped(pos.slot) = req.value;
                    update_success_l++;
                }else{
                    update_failure_l++;
                }
            }
            break;
        case Erase :{
            table_position pos = insert_func(req);
            if(pos.status == failure_key_duplicated){
                //buckets_[pos.index].mapped(pos.slot) = req.value;
                buckets_.eraseKV(pos.index, pos.slot);
                erase_success_l++;
            }else{
                erase_failure_l++;
            }
            }
            break;

    }
    if(req.index == BOTH_BUCKET){
        locks[0].unlock();
        locks[1].unlock();
    }else{
        locks[req.index].unlock();
    }
}

void worker(int tid){

    Tracer t;
    t.startTime();

    while (stopMeasure.load(std::memory_order_relaxed) == 0) {
        for(size_t i = 0 ; i < total_count ; i++){
            op_func(requests[i]);
        }

        __sync_fetch_and_add(&op_num,total_count);

        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 >= timer_range){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    merge_log();
    runtimelist[tid] = t.getRunTime();
}

int main(int argc, char **argv){
    if (argc == 5) {
        thread_num = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
    }
    std::cout<<" thread_num "<<thread_num
             <<" key_range "<<key_range
             <<" total_count "<<total_count
             <<" timer_range "<<timer_range<<std::endl;

    std::cout<<" bucket size:"<<buckets_.size()<<std::endl;

    srand((unsigned)time(NULL));
    //init_req
    static_assert(op_type_num == 5 && bucket_type_num == 3);
    requests = new Request[total_count];
    for(size_t i = 0; i < total_count;i++){
        requests[i].optype = Op_type(rand() % op_type_num);
        requests[i].key = rand() % key_range;
        requests[i].index = Bucket_type(requests[i].key % bucket_type_num );
        requests[i].value = VALUE_DEFAULT;
    }

    runtimelist = new uint64_t[thread_num]();

    std::vector<std::thread> threads;
    for(int i = 0;i < thread_num;i++) threads.emplace_back(std::thread(worker,i));
    for(int i = 0;i < thread_num;i++) threads[i].join();

    show_info();

}


