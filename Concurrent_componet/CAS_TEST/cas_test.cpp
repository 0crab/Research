#include <iostream>
#include <thread>
#include <vector>

#include "tracer.h"

//#define GLOBAL 1

using namespace std;

int thread_num;
int reader_num;
int writer_num;
uint64_t test_num;
int conflict_rate;
int write_rate;

uint64_t stop_flag = 0;

#define THREAD_NUM thread_num
#define READER_NUM reader_num
#define WRITER_NUM writer_num
#define TEST_NUM test_num

//#define UPGRADE
#define R_LOOP 1
#define W_LOOP 1

uint64_t outvalue = 0;
#ifdef GLOBAL
struct alignas(128) OPTYPE{
        uint64_t value;
};
alignas(128) OPTYPE *  oplist;
#endif

unsigned long *runtimelist;
bool * conflict_signal_list;
bool * write_signal_list;

typedef struct alignas(128) {
    atomic<uint64_t *> opobj;
}op;

vector<op> oplist;

int reader(int tid, int limit) {
    ;
}



int writer(int tid, int limit){
    bool *local_conflict_list;
    bool *local_write_list;
    local_conflict_list = static_cast<bool *>(calloc(TEST_NUM, sizeof(bool)));
    local_write_list =  static_cast<bool *>(calloc(TEST_NUM, sizeof(bool)));
    memcpy(local_conflict_list,conflict_signal_list,TEST_NUM*sizeof(bool));
    memcpy(local_write_list,write_signal_list,TEST_NUM*sizeof(bool));

    Tracer tracer;
    tracer.startTime();
    uint64_t value=0;
    double op = 1.0;
    for (int i = 0; i < limit; ++i) {
        if(local_write_list[i]){
            uint64_t *tmp = new uint64_t(i);
            if(local_conflict_list[i]){
                //__sync_fetch_and_add(&oplist[THREAD_NUM].opobj,2);
                while(true){
                    uint64_t * expected  = oplist[THREAD_NUM].opobj.load();
                    if(oplist[THREAD_NUM].opobj.compare_exchange_strong(expected,tmp)){
                        break;
                    }
                }
            }else{
                while(true){
                    uint64_t * expected  = oplist[tid].opobj.load();
                    if(oplist[tid].opobj.compare_exchange_strong(expected,tmp)){
                        break;
                    }
                }
                //__sync_fetch_and_add(&oplist[tid].opobj,2);
            }
        }else{
            if(local_conflict_list[i]){
                value += *oplist[THREAD_NUM].opobj.load();
            }else{
                value += *oplist[tid].opobj.load();
            }
        }

    }
    //value = (uint64_t) op;
    runtimelist[tid]+=tracer.getRunTime();
    outvalue += value;// Prevent optimization
    return 0;
}


int main(int argc,char **argv) {
    if(argc == 6){
        thread_num = atol(argv[1]);
        writer_num = atol(argv[2]);
        test_num = atol(argv[3]);
        conflict_rate = atol(argv[4]);
        write_rate = atol(argv[5]);
    }else{
        printf("./locks <thread_num>  <writer_num> <test_num> <conflict_rate> <write_rate>\n");
        exit(0);
    }

    reader_num = thread_num - writer_num;

    cout<<"thread_num:\t"<<thread_num<<endl;
    cout<<"writer_num:\t"<<writer_num<<endl;
    cout<<"reader_num:\t"<<reader_num<<endl;
    cout<<"test_num per thread:\t"<<test_num<<endl;
    cout<<"conflict_rate:\t"<<conflict_rate<<endl;
    cout<<"write_rate\t"<<write_rate<<endl;


    vector<op> tmp(THREAD_NUM+1);
    oplist.swap(tmp);
    for(int i = 0;i< THREAD_NUM+1;i++){
        uint64_t *tmp = new uint64_t(i);
        oplist[i].opobj.store(tmp);
    }


    runtimelist = static_cast<unsigned long *>(calloc(THREAD_NUM, sizeof(uint64_t)));

    //conflict_rate list initialize
    conflict_signal_list = static_cast<bool *>(calloc(TEST_NUM, sizeof(bool)));
    write_signal_list = static_cast<bool *>(calloc(TEST_NUM, sizeof(bool)));
    srand((unsigned)time(NULL));
    for(int i = 0 ; i < TEST_NUM; i++){
//      if(rand()%100<10){
        conflict_signal_list[i] = rand() % 100 < conflict_rate;
        write_signal_list[i] = rand() % 100 < write_rate;
//      }else{
//              conflict_signal_list[i] = false;
//      }
    }


    vector<thread> threads;
    for (int i = 0; i < THREAD_NUM; i++) {
        if(i < WRITER_NUM){
            threads.push_back(thread(writer, i, TEST_NUM));
        }else{
            threads.push_back(thread(reader, i, TEST_NUM));
        }
    }

    for (int i = 0; i < THREAD_NUM; i++) threads[i].join();


    //uint64_t avg_spin = locks[THREAD_NUM].spin_count / (THREAD_NUM * TEST_NUM);
    //cout<<"*avg_spin:"<<avg_spin<<endl;

    uint64_t runtime = 0;
    double throughput = 0.0;
    for(int i = 0 ; i < THREAD_NUM; i++) runtime +=runtimelist[i];
    runtime /= THREAD_NUM;
    throughput = ( TEST_NUM * THREAD_NUM ) * 1.0 / runtime;

#ifdef GLOBAL
    for(int i = 0; i < THREAD_NUM + 1; i++) outvalue+=oplist[i].value;
#endif

    std::cout << outvalue << std::endl;
    cout<<"runtime:"<<runtime<<endl;
    cout<<"*****throughput:["<<throughput<<"]"<<endl<<endl;
}
