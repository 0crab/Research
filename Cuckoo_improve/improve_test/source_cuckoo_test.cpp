#include <iostream>
#include <random>
#include <vector>
#include <mutex>
#include <cstring>
#include <atomic>
#include <unordered_set>
#include "generator.h"
#include "tracer.h"

//#include "new_map.hh"
#include "../libcuckoo_source/cuckoohash_map.hh"
#include "assert_msg.h"
#include "ycsb_loader.h"


using namespace std;

constexpr uint32_t kHashSeed = 7079;

uint64_t MurmurHash64A(const void *key, size_t len) {
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

template<typename T>
struct str_equal_to {
    bool operator()(const T first, T second) {
        return !std::memcmp(first, second, 8);
    }
};

template<typename T>
struct str_hash {
    std::size_t operator()(T str) const noexcept {
        return MurmurHash64A(str, 8);
    }
};

typedef libcuckoo::cuckoohash_map<char *, char *, str_hash<char *>, str_equal_to<char *>, std::allocator<std::pair<const char *, char *>>> cmap;

cmap *store;
//new_cuckoohash_map store(1);

static const int op_type_num = 4;
enum Op_type {
    Find = 0,
    Set = 1, // Insert_or_Assign
//    Update = 3,
    Erase = 2,
    Insert = 3,
    Rand = 4
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
Request *requests_run;

Request *loads;
std::vector<YCSB_request *> ycsb_loads;
std::vector<YCSB_request *> ycsb_requests;


bool YCSB;
int thread_num = 1;
int insert_thread_num = 1;
size_t init_hashpower = 1;
size_t key_range = 1;
size_t total_count = 1;
int timer_range = 0;
int distribution = 0; // 0 unif; 1 zipf
Op_type op_chose = Rand;

static size_t find_success, find_failure;
static size_t insert_success, insert_failure;
static size_t set_insert, set_assign;
static size_t update_success, update_failure;
static size_t erase_success, erase_failure;
static size_t kick_num,
                depth0, // ready to kick, then find empty slot
                kick_lock_failure_data_check,
                kick_lock_failure_haza_check,
                kick_lock_failure_other_lock,
                kick_lock_failure_haza_check_after,
                kick_lock_failure_data_check_after,
                key_duplicated_after_kick;

size_t kick_path_length_log[6];

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


template<typename R>
class RandomGenerator {
public:
    static inline void generate(R *array, size_t range, size_t count, double skew = 0.0) {
        struct stat buffer;
        if (skew < zipf_distribution<R>::epsilon) {
            std::default_random_engine engine(
                    static_cast<R>(chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<size_t> dis(0, range + FUZZY_BOUND);
            for (size_t i = 0; i < count; i++) {
                array[i] = static_cast<R>(dis(engine));
            }
        } else {
            zipf_distribution<R> engine(range, skew);
            mt19937 mt;
            for (size_t i = 0; i < count; i++) {
                array[i] = engine(mt);
            }
        }
    }
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


void op_func(const Request &req) {

    Op_type switch_option = op_chose == Rand ? static_cast<Op_type>(rand() % 3) : op_chose;

    switch (switch_option) {

        //switch(Find){
        case Find : {

            if (store->contains(req.key)){
                find_success_l++;
            }else{
                find_failure_l++;
            }
        }
            break;
        case Insert : {
            if (store->insert(req.key,  req.value)) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }
            break;
        case Set : {
            if (store->insert_or_assign(req.key, req.value)) {
                set_assign_l++;
            } else {
                set_insert_l++;
            }
        }
            break;

    }

}

void ycsb_op_func(YCSB_request * req){
//    switch (req->getOp()) {
//
//        //switch(Find){
//        case lookup : {
//            if (store.find(req->getKey(), req->keyLength()-4))
//                find_success_l++;
//            else
//                find_failure_l++;
//        }
//            break;
//        case insert : {
//            if (store.insert(req->getKey(), req->keyLength()-4, req->getVal(), req->valLength())) {
//                insert_success_l++;
//            } else {
//                insert_failure_l++;
//            }
//        }
//            break;
//        case update : {
//            if (store.insert_or_assign(req->getKey(), req->keyLength()-4, req->getVal(), req->valLength())) {
//                set_insert_l++;
//            } else {
//                set_assign_l++;
//            }
//        }
//            break;
//        default:
//            ASSERT(false,"optype error");
//
//    }
}


void insert_worker(int tid){
    //thread_id = tid;

    //Prevent tail debris
    size_t step =  total_count / insert_thread_num;
    size_t num = tid == insert_thread_num -1 ?  step + total_count % insert_thread_num : step;
    size_t base = tid * step;

    for (size_t i = 0; i < num ; i++) {
        if(!YCSB){
            auto &req = requests[base + i];
            if (store->insert(req.key, req.value)) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }else{

        }

    }


    __sync_fetch_and_add(&insert_success, insert_success_l);
    __sync_fetch_and_add(&insert_failure, insert_failure_l);

}

void worker(int tid) {
//    thread_id = tid;

    size_t step =  total_count / thread_num;
    size_t num = tid == thread_num -1 ?  step + total_count % thread_num : step;
    size_t base = tid * step;

    Tracer t;
    t.startTime();

    while (stopMeasure.load(std::memory_order_relaxed) == 0) {

        for (size_t i = 0; i < num; i++) {
            if(!YCSB){
                op_func(requests_run[base + i]);
            }else{
                ycsb_op_func(ycsb_requests[base + i]);
            }

        }

        __sync_fetch_and_add(&op_num, num);

        uint64_t tmptruntime = t.fetchTime();
        if (tmptruntime / 1000000 >= timer_range) {
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    merge_log();
    runtimelist[tid] = t.getRunTime();
}



void prepare(){
    if(!YCSB){
        double skew = distribution == 0? 0.0:0.99;

        uint64_t *loads = new uint64_t[total_count]();
        RandomGenerator<uint64_t>::generate(loads,key_range,total_count,skew);

        srand((unsigned) time(NULL));
        //init_req
        static_assert(op_type_num == 4);
        requests = new Request[total_count];
        requests_run = new Request[total_count];
        for (size_t i = 0; i < total_count; i++) {
            requests[i].optype = Find;//rand() % 2 == 0? Find : Set;

            requests[i].key = (char *) calloc(1, 8 * sizeof(char));
            requests[i].key_len = default_key_len;
            *((size_t *) requests[i].key) = loads[i];

            requests[i].value = (char *) calloc(1, 8 * sizeof(char));
            requests[i].value_len = default_value_len;
            *((size_t *) requests[i].value) = loads[i];

        }
        memcpy(requests_run,requests,total_count * sizeof (Request));

        delete[] loads;
    }else{
//        YCSBLoader loader(load_filepath.c_str());
//        ycsb_loads=loader.load();
//        total_count = ycsb_loads.size();
//
//        YCSBLoader loader1(run_filepath.c_str());
//        ycsb_requests=loader1.load();
//        ASSERT(total_count == ycsb_requests.size(),"total count error");
//        std::cout<<"total_count: "<<total_count<<std::endl;
    }


}

bool check_unique();
void show_info_insert();
void show_info_before();
void show_info_after();
void prepare();

int main(int argc, char **argv) {
    if (argc == 9) {
        insert_thread_num = std::atol(argv[1]);
        thread_num = std::atol(argv[2]);
        init_hashpower = std::atol(argv[3]);
        op_chose = static_cast<Op_type>(std::atol(argv[4]));
        key_range = std::atol(argv[5]);
        total_count = std::atol(argv[6]);
        distribution = std::atol(argv[7]);
        timer_range = std::atol(argv[8]);
        YCSB = false;
    } else if(argc == 5){
        insert_thread_num = std::atol(argv[1]);
        thread_num = std::atol(argv[2]);
        init_hashpower = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        YCSB = true;
    }else{
        cout << "micro_benchmark:"<<endl;
        cout << "./a.out <insert_thread_num> <thread_num> <init_hashpower> <op_chose> <key_range>"
                "<total_count> <distribution> <timer_range>" << endl;
        cout << "ycsb:"<<endl;
        cout << "./a.out <insert_thread_num> <thread_num> <init_hashpower> <timer_range>" << endl;
        cout << "op_chose    :0-Find,1-Set,2-Erase,3-Insert,4-Rand " << endl;
        cout << "distribution:0-unif,1-zipf" << endl;

        exit(-1);
    }

    show_info_before();

    store = new cmap((1ull<<init_hashpower) * 4);

    std::cout << "real hashpower " << store->hashpower() << std::endl;
    std::cout << "total_slot_num " << store->capacity() << std::endl;

    prepare();

    uint64_t items_in_table_brf = store->size();

    std::vector<std::thread> insert_threads;
    for (int i = 0; i < insert_thread_num; i++) insert_threads.emplace_back(std::thread(insert_worker, i));
    for (int i = 0; i < insert_thread_num; i++) insert_threads[i].join();

    show_info_insert();

    uint64_t items_in_table = store->size();


    std::random_shuffle(requests_run, requests_run + total_count);


    runtimelist = new uint64_t[thread_num]();

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num; i++) threads.emplace_back(std::thread(worker, i));
    for (int i = 0; i < thread_num; i++) threads[i].join();


    show_info_after();

}

void show_info_insert(){

    cout << ">>>>>pre insert finish" <<"\tinsert_success: "<<insert_success<<"\tkick_num: "<<kick_num<< endl;

}

void show_info_before() {
    if(YCSB){
//        std::cout << " thread_num " << thread_num
//                 << " init_hashpower " << init_hashpower
//                 << " timer_range " << timer_range << std::endl;
//        std::cout << "---YCSB--- "<<std::endl;
//        std::cout << "loadpath:\t"<<load_filepath<<std::endl;
//        std::cout << "runpath:\t"<<run_filepath<<std::endl;
//        std::cout << "loading file... "<<std::endl;
    }else{
        string op_chose_str;
        switch (op_chose) {
            case Find:
                op_chose_str = "Find";
                break;
            case Set:
                op_chose_str = "Set";
                break;
            case Erase:
                op_chose_str = "Erase";
                break;
            case Insert:
                op_chose_str = "Insert";
                break;
            case Rand:
                op_chose_str = "Rand";
                break;
            default:
                ASSERT(false, "op_chose not defined");
        }

        ASSERT(distribution == 0 || distribution == 1, "distribution not defined");
        string distribution_str = distribution == 0 ? "uinf" : "zipf";

        std::cout << " thread_num " << thread_num
                  << " init_hashpower " << init_hashpower
                  << " op_chose " << op_chose_str
                  << " key_range " << key_range
                  << " total_count " << total_count
                  << " distribution " << distribution_str
                  << " timer_range " << timer_range << std::endl;



    }

}

void show_info_after() {

    std::cout << " find_success " << find_success << "\tfind_failure " << find_failure << std::endl;
    std::cout << " insert_success " << insert_success << "\tinsert_failure " << insert_failure << std::endl;
    std::cout << " set_insert " << set_insert << "\tset_assign " << set_assign << std::endl;
    std::cout << " update_success " << update_success << "\tupdate_failure " << update_failure << std::endl;
    std::cout << " erase_success " << erase_success << "\terase_failure " << erase_failure << std::endl;



    std::cout << endl << " op_num " << op_num << std::endl;
    std::cout << "occupancy "  << store->load_factor() << std::endl;

    uint64_t runtime = 0;
    for (int i = 0; i < thread_num; i++) {
        runtime += runtimelist[i];
    }
    runtime /= thread_num;
    std::cout << " runtime " << runtime << std::endl;

    double throughput = op_num * 1.0 / runtime;
    std::cout << "***throughput " << throughput << std::endl;


}

