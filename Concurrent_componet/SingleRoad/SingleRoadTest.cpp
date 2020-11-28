#include <iostream>
#include <thread>
#include "tracer.h"

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
    atomic<uint64_t * > vp;
};

KV_OBJ * kvlist;

bool * conflictlist;
bool * writelist;
uint64_t *runtimelist;

atomic<int> stopMeasure(0);
uint64_t runner_count;

struct alignas(128) R_BUF{
    uint64_t r_buf;
};

R_BUF * r_bufs;


inline void store_value(int tid, int index , uint64_t * v) {
    while (true) {
        uint64_t *expected = kvlist[index].vp.load();
        if (kvlist[index].vp.compare_exchange_strong(expected, v)) {
            //GC expected ptr
            break;
        }
    }
}

inline void read_value(int tid, int index , uint64_t * v){
    *v = * kvlist[index].vp.load();
}

void concurrent_worker(int tid){
    Tracer t;
    t.startTime();
    while(stopMeasure.load(memory_order_relaxed) == 0){

        for(size_t i = 0; i < TEST_NUM; i++){
            if(writelist[i]){
                uint64_t * tmp = new uint64_t (i);
                if(conflictlist[i]){
                    store_value(tid,THREAD_NUM,tmp);
                }else{
                    store_value(tid,tid,tmp);
                }
            }else{
                if(conflictlist[i]){
                    read_value(tid,THREAD_NUM,&r_bufs[tid].r_buf);
                }else{
                    read_value(tid,tid,&r_bufs[tid].r_buf);
                }
            }
        }

        __sync_fetch_and_add(&runner_count,TEST_NUM);
        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 > TEST_TIME){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    runtimelist[tid] = t.getRunTime();
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

    runtimelist = new uint64_t[THREAD_NUM]();

    r_bufs = new R_BUF[THREAD_NUM];

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
    cout<<"runtime "<<runtime / 1000000<<"s"<<endl;
    cout<<"***throughput "<<throughput<<endl<<endl;

}