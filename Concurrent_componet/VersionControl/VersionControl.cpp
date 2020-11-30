#include <iostream>
#include <thread>
#include "tracer.h"
#include "version_control.h"

#define VPP 1

using namespace std;

static int THREAD_NUM ;
static int TEST_NUM;
static int TEST_TIME;
static double CONFLICT_RATIO;
static double WRITE_RATIO;

//static char test_str[9] = "deedbeef";

static const uint64_t key_demo = 12345678;

//static const int value_para_num = 1;


struct alignas(128) KV_OBJ{
    uint64_t key;
    uint64_t * vp;
};

KV_OBJ * kvlist;
AtomicGenLock * locks;


uint64_t ** opvaluelist;

bool * conflictlist;
bool * writelist;
uint64_t *runtimelist;

atomic<int> stopMeasure(0);
uint64_t runner_count;

uint64_t g_value;

struct alignas(128) R_BUF{
    uint64_t r_buf;
};


void concurrent_worker(int tid){
    uint64_t l_value = 0;
    int index = 0;
    Tracer t;
    t.startTime();
    while(stopMeasure.load(memory_order_relaxed) == 0){
        for(size_t i = 0; i < TEST_NUM; i++){
            if(writelist[i]){
                index = conflictlist[i] ? THREAD_NUM : tid;
                bool replaced = false;
                while (!locks[index].try_lock(replaced) && !replaced) {
                    std::this_thread::yield();
                }
#ifdef VPP
                l_value++;
#else
                * kvlist[index].vp = * opvaluelist[i];
#endif
                locks[index].unlock(false);

            }else{
                index = conflictlist[i] ? THREAD_NUM : tid;
                GenLock before, after;
                do {
                    before = locks[index].load();
#ifdef VPP
                    l_value;
#else
                    l_value += * kvlist[index].vp;
#endif
                    do {
                        after = locks[index].load();
                    } while (after.locked /*|| after.replaced*/);
                } while (before.gen_number != after.gen_number);
            }
        }

        __sync_fetch_and_add(&runner_count,TEST_NUM);
        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 > TEST_TIME){
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

    //init kvlist
    kvlist = new KV_OBJ[THREAD_NUM + 1];
    for(size_t i = 0; i < THREAD_NUM + 1; i++) {
        uint64_t * tmp = new uint64_t(0);
        kvlist[i].key = key_demo;
        kvlist[i].vp = tmp ;
    }

    locks = new AtomicGenLock[THREAD_NUM + 1];

    opvaluelist = new uint64_t *[TEST_NUM];
    for(size_t i = 0;i < TEST_NUM;i++){
        opvaluelist[i] = new uint64_t(i);
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