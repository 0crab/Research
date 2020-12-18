#include <iostream>
#include <random>
#include <vector>
#include <mutex>
#include <atomic>
#include "tracer.h"
#include "item.h"

#include "new_map.hh"
#include "assert_msg.h"

using namespace libcuckoo;

new_cuckoohash_map store(1);

static const int op_type_num = 4;
enum Op_type {
    Find = 0,
    Set = 1, // Insert_or_Assign
//    Update = 3,
    Erase = 2,
    Insert = 3
};


static const uint16_t default_key_len = 8;
static const uint16_t default_value_len = 8;

typedef struct Request {
    Op_type optype;
    char *key;
    uint16_t key_len;
    char *value;
    uint16_t value_len;
};

Request *requests;

int thread_num = 1;
size_t init_hashpower = 1;
size_t key_range = 1;
size_t total_count = 1;
int timer_range = 0;

static size_t find_success, find_failure;
static size_t insert_success, insert_failure;
static size_t set_insert, set_assign;
static size_t update_success, update_failure;
static size_t erase_success, erase_failure;

thread_local static size_t find_success_l, find_failure_l;
thread_local static size_t insert_success_l, insert_failure_l;
thread_local static size_t set_insert_l, set_assign_l;
thread_local static size_t update_success_l, update_failure_l;
thread_local static size_t erase_success_l, erase_failure_l;


uint64_t *runtimelist;
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

inline void merge_log() {
    __sync_fetch_and_add(&find_success, find_success_l);
    __sync_fetch_and_add(&find_failure, find_failure_l);
    __sync_fetch_and_add(&insert_success, insert_success_l);
    __sync_fetch_and_add(&insert_failure, insert_failure_l);
    __sync_fetch_and_add(&set_insert, set_insert_l);
    __sync_fetch_and_add(&set_assign, set_assign_l);
    __sync_fetch_and_add(&update_success, update_success_l);
    __sync_fetch_and_add(&update_failure, update_failure_l);
    __sync_fetch_and_add(&erase_success, erase_success_l);
    __sync_fetch_and_add(&erase_failure, erase_failure_l);
}

void show_info() {
    std::cout << " find_success " << find_success << "\tfind_failure " << find_failure << std::endl;
    std::cout << " insert_success " << insert_success << "\tinsert_failure" << insert_failure << std::endl;
    std::cout << " set_insert " << set_insert << "\tset_assign " << set_assign << std::endl;
    std::cout << " update_success " << update_success << "\tupdate_failure " << update_failure << std::endl;
    std::cout << " erase_success " << erase_success << "\terase_failure " << erase_failure << std::endl;

    uint64_t item_num = store.get_item_num();
    std::cout << "items in table " << item_num << std::endl;

    std::cout << endl << " op_num " << op_num << std::endl;

    uint64_t runtime = 0;
    for (int i = 0; i < thread_num; i++) {
        runtime += runtimelist[i];
    }
    runtime /= thread_num;
    std::cout << " runtime " << runtime << std::endl;

    double throughput = op_num * 1.0 / runtime;
    std::cout << "***throughput " << throughput << std::endl;


    ASSERT(op_num == find_success + find_failure
                     + set_insert + set_assign
                     + erase_success + erase_failure, "op_num not correct");

    ASSERT(insert_success + set_insert - erase_success == item_num, "item != inert - erase");


}


void op_func(const Request &req) {


    switch (rand() % 3) {

        //switch(Find){
        case Find : {
            if (store.find(req.key, req.key_len))
                find_success_l++;
            else
                find_failure_l++;
        }
            break;
        case Insert : {
            if (store.insert(req.key, req.key_len, req.value, req.value_len)) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }
            break;
        case Set : {
            if (store.insert_or_assign(req.key, req.key_len, req.value, req.value_len)) {
                set_insert_l++;
            } else {
                set_assign_l++;
            }
        }
            break;
//        case Update :{
//                partial_t partial = (partial_t) req.key;
//                table_position pos = insert_func(req);
//                if(pos.status == failure_key_duplicated){
//                    buckets_[pos.index].mapped(pos.slot) = req.value;
//                    update_success_l++;
//                }else{
//                    update_failure_l++;
//                }
//            }
//            break;
        case Erase : {
            if (store.erase(req.key, req.key_len)) {
                erase_success_l++;
            } else {
                erase_failure_l++;
            }
        }
            break;

    }

}

void worker(int tid) {

    Tracer t;
    t.startTime();

    while (stopMeasure.load(std::memory_order_relaxed) == 0) {
        for (size_t i = 0; i < total_count; i++) {
            op_func(requests[i]);
        }

        __sync_fetch_and_add(&op_num, total_count);

        uint64_t tmptruntime = t.fetchTime();
        if (tmptruntime / 1000000 >= timer_range) {
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    merge_log();
    runtimelist[tid] = t.getRunTime();
}

int main(int argc, char **argv) {
    if (argc == 6) {
        thread_num = std::atol(argv[1]);
        init_hashpower = std::atol(argv[2]);
        key_range = std::atol(argv[3]);
        total_count = std::atol(argv[4]);
        timer_range = std::atol(argv[5]);
    }else{
        cout<<"./a.out <thread_num> <init_hashpower> <key_range> <total_count> <timer_range>"<<endl;
        exit(-1);
    }

    std::cout << " thread_num " << thread_num
              << " init_hashpower "<<init_hashpower
              << " key_range " << key_range
              << " total_count " << total_count
              << " timer_range " << timer_range << std::endl;

    {
        new_cuckoohash_map tmp(init_hashpower);
        store.swap(tmp);
    }
    std::cout << "bucket hashpower:" << store.hashpower()<< std::endl;
    uint64_t total_slot_num = 4 * (1 << init_hashpower);
    std::cout <<"total_slot_num " << total_slot_num <<std::endl;


    srand((unsigned) time(NULL));
    //init_req
    static_assert(op_type_num == 4);
    requests = new Request[total_count];
    for (size_t i = 0; i < total_count; i++) {
        requests[i].optype = Find;//rand() % 2 == 0? Find : Set;

        requests[i].key = (char *) calloc(1, 8 * sizeof(char));
        requests[i].key_len = default_key_len;
        char *&keybuf = requests[i].key;
        memset(keybuf, '*', default_key_len);
        sprintf(keybuf, "%d", i);

        requests[i].value = (char *) calloc(1, 8 * sizeof(char));
        requests[i].value_len = default_value_len;
        char *&valuebuf = requests[i].value;
        memset(valuebuf, '@', default_value_len);
        sprintf(valuebuf, "%d", i);
    }

    for (size_t i = 0; i < total_count; i++) {
        auto &req = requests[i];
        if (store.insert(req.key, req.key_len, req.value, req.value_len)) {
            insert_success++;
        } else {
            insert_failure++;
        }
    }

    cout << "pre insert finish" << endl;

    runtimelist = new uint64_t[thread_num]();

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num; i++) threads.emplace_back(std::thread(worker, i));
    for (int i = 0; i < thread_num; i++) threads[i].join();

    show_info();

}


