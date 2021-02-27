#ifndef MY_RECLAIMER_MULTI_LEVEL_QUEUE_H
#define MY_RECLAIMER_MULTI_LEVEL_QUEUE_H

#include "../item.h"
#include "buffer_queue.h"



template <typename T>
class MultiLevelQueue{
public:
    MultiLevelQueue();

    T * allocate(uint64_t len);
    void deallocate(T * ptr);
    void free_limbobag(BufferQueue<T> * freebag);

    uint64_t get_total_size();
    void dump();

private:

    uint64_t size; //total number of items in this multi level queue
    BufferQueue<T> BFQS[MAX_ORDER];

};

template <typename T>
MultiLevelQueue<T>::MultiLevelQueue() {
    size = 0;
    //bfq.size,listhead has been initialized
    for(int i = 0 ; i < MAX_ORDER; i++ ){
        BFQS[i].set_unif_size(ORDER(i));
    }
}

template <typename T>
uint64_t MultiLevelQueue<T>::get_total_size() {
    return size;

}

template<typename T>
T * MultiLevelQueue<T>::allocate(uint64_t len) {

    //size is too large,malloc directly
    if(len > MAX_UNIF_SIZE){
        void * tp=malloc(len + 2 * sizeof(POINTER)); //buffer size + two pointer spaces used to construct lists
        ((Listhead *)tp)->prev= nullptr;
        ((Listhead *)tp)->next= nullptr;
        return (T*)((POINTER)tp+2);
    }

    T *p;
    //serch in bufqueue
    //since small unif_sizes are used more frequently,we start with the header
    int i=0;
    while(BFQS[i].get_unif_size() < len) i++;

    assert(i < MAX_ORDER);
    bool malloc_flag = false;
    p = BFQS[i].remove(len,malloc_flag);
    if(!malloc_flag) size--;
    assert(size >= 0);

    return p;

}


//only used in reclaimering freebag
template<typename T>
void MultiLevelQueue<T>::deallocate(T *ptr) {
    uint64_t len = ptr->get_struct_len();

    if(len>MAX_UNIF_SIZE){
        void * tp = (POINTER)ptr-2;
        free(tp);
        return;
    }

    int i=0;
    while(BFQS[i].get_unif_size()<len) i++;

    BFQS[i].add(ptr,len);
    size++;
}


template<typename T>
void MultiLevelQueue<T>::free_limbobag(BufferQueue<T> * freebag) {

    while(freebag->get_size() > 0){
        T * p = freebag->pop();
        tw_info.num_mlq_reclaim++;
        deallocate(p);
    }
}

template<typename T>
void MultiLevelQueue<T>::dump() {
    cout<<"mlq total size: "<<size<<endl;
    for(int i = 0; i < MAX_ORDER; i++){
        BufferQueue<T> & bfq = BFQS[i];
        cout<<i<<" : "<<bfq.get_unif_size()<<" : "<<bfq.get_size()<<endl;
    }
}

#endif //MY_RECLAIMER_MULTI_LEVEL_QUEUE_H
