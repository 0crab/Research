#include <iostream>
#include <thread>
#include "tracer.h"
#include "brown_reclaim.h"

//#define VPP 1

using namespace std;

static int THREAD_NUM ;
static int TEST_NUM;
static int TEST_TIME;
static double CONFLICT_RATIO;
static double WRITE_RATIO;

uint64_t ** opvaluelist;

bool * conflictlist;
bool * writelist;
uint64_t *runtimelist;

atomic<int> stopMeasure(0);
uint64_t runner_count;

uint64_t g_value;

class node {
public:
    uint64_t key;
    uint64_t value;
public:
    node() : key(-1), value(-1) {}

    ~node() { value = -1; }
};

static const int align_with = 10;
static int bucket_num;

std::atomic<uint64_t> *bucket;

ihazard<node> *deallocator;

#define alloc allocator_new
#define pool pool_perthread_and_shared

typedef brown_reclaim<node , alloc<node>, pool<>, reclaimer_hazardptr<>> brown6;

void concurrent_worker(int tid){
    deallocator->initThread(tid);
    uint64_t l_value=0;
    int index = 0;
    Tracer t;
    t.startTime();
    while(stopMeasure.load(memory_order_relaxed) == 0){
        for(size_t i = 0; i < TEST_NUM; i++){
            if(writelist[i]){
                index = conflictlist[i] ? THREAD_NUM * align_with : tid * align_with;

                node *ptr = (node *) deallocator->allocate(tid);
                ptr->key = index ;
                ptr->value = 1;

                uint64_t old;
                do{
                    old = bucket[index].load();
                }while(!bucket[index].compare_exchange_strong(old, (uint64_t) ptr)); //changed memory order
                node *oldptr = (node *) old;
                assert(oldptr->value == 1);
                deallocator->free(old);

            }else{
                index = conflictlist[i] ? THREAD_NUM * align_with: tid * align_with;

                node *ptr = (node *) deallocator->load(tid, std::ref(bucket[index]));

                l_value += ptr->value;
                deallocator->read(tid);
            }
        }

        __sync_fetch_and_add(&runner_count,TEST_NUM);
        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 >= TEST_TIME){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    runtimelist[tid] = t.getRunTime();
    g_value += l_value;
}


int main(int argc, char **argv){
    if (argc == 6) {
        THREAD_NUM = stol(argv[1]);
        TEST_TIME = stol(argv[2]);
        TEST_NUM = stol(argv[3]);
        CONFLICT_RATIO = stod(argv[4]);
        WRITE_RATIO = stod(argv[5]);
    } else {
        printf("./kv_rw <thread_num>  <test_time> <test_num> <conflict_ratio> <write_ratio>\n");
        return 0;
    }

    cout<<"thread_num "<<THREAD_NUM<<endl<<
        "test_time "<<TEST_TIME<<endl<<
        "test_num "<<TEST_NUM<<endl<<
        "conflict_ratio "<<CONFLICT_RATIO<<endl<<
        "write_ratio "<<WRITE_RATIO<<endl;

    deallocator = new brown6(THREAD_NUM);

    bucket_num = (THREAD_NUM+1)*align_with;
    bucket = new std::atomic<uint64_t>[bucket_num];
    deallocator->initThread();
    for (size_t i = 0; i < bucket_num ; i++) {
        node *ptr;
        ptr = (node *) deallocator->allocate(0);
        size_t idx = i;
        ptr->key = idx;
        ptr->value = 1;
        bucket[idx].store((uint64_t) ptr);
    }

    g_value = 0;

    runtimelist = new uint64_t[THREAD_NUM]();


    srand(time(NULL));
    conflictlist = new bool[TEST_NUM];
    writelist = new bool[TEST_NUM];
    for(size_t i = 0; i < TEST_NUM; i++ ){
        conflictlist[i] = rand() * 1.0 / RAND_MAX * 100  < CONFLICT_RATIO;
        writelist[i] = rand() *1.0 / RAND_MAX * 100 < WRITE_RATIO;
    }

    vector<thread> threads;
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads.push_back(thread(concurrent_worker,i));
    }
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads[i].join();
    }

    double runtime = 0 , throughput = 0;
    for(size_t i = 0 ; i < THREAD_NUM; i++)
        runtime += runtimelist[i];
    runtime /= THREAD_NUM;
    throughput = runner_count * 1.0 / runtime;
    cout<<"runner_count "<<runner_count<<endl;
    cout<<"g_value "<<g_value<<endl;
    cout<<"runtime "<<runtime / 1000000<<"s"<<endl;
    cout<<"***throughput "<<throughput<<endl<<endl;

}