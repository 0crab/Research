//
// Created by Michael on 11/26/2019.
//

#ifndef HASHCOMP_ADAPTIVE_HAZARD_H
#define HASHCOMP_ADAPTIVE_HAZARD_H

#include <cstring>
#include "hash_hazard.h"
#include "memory_hazard.h"

thread_local uint64_t skew = 0;

thread_local uint64_t tick = 0;

char information[255];

template<class T, class D = T>
class adaptive_hazard : public hash_hazard<T, D> {
private:
    holder lrulist[thread_limit];
    holder holders[thread_limit];
    uint64_t intensive_high;
    uint64_t switch_period;

    indicator readintensive[thread_limit], writeintensive[thread_limit];

    alignas(128) std::atomic<uint64_t> intensive{0};

protected:
    using hash_hazard<T, D>::total_holders;
    using hash_hazard<T, D>::indicators;
    using ihazard<T, D>::thread_number;

public:
    adaptive_hazard<T, D>(size_t total_thread) : hash_hazard<T, D>(total_thread) {
        for (size_t i = 0; i < total_thread; i++) {
            holders[i].init();
            lrulist[i].init();
            readintensive[i].init();
            writeintensive[i].init();
        }
        intensive_high = 0;
        switch_period = thread_number * 32;
        for (int i = 0; i < total_thread; i++) {
            intensive_high |= (1llu << i);
        }
        std::cout << "adaptive scheme enabled: " << intensive_high << std::endl;
    }

    ~adaptive_hazard() {}

    const char *info() {
        ::memset(information, 0, 255);
        uint64_t totalreadintensive = 0, totalwriteintensive = 0;
        for (size_t i = 0; i < thread_number; i++) {
            totalreadintensive += readintensive[i].load();
            totalwriteintensive += writeintensive[i].load();
        }
        std::sprintf(information, "%llu, %llu", totalreadintensive, totalwriteintensive);
        return information;
    }

    void registerThread() {}

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load(std::memory_order_relaxed);
        if (tick++ % switch_period == 0) {
            lrulist[tid].store(address);
            uint64_t other = lrulist[(tid + thread_number / 2) % thread_number].load();
            if (other == address) {
                if (skew == 0) {
                    uint64_t bit = 1llu << tid;
                    intensive.fetch_or(bit, std::memory_order_relaxed);
                    skew = 1;
                }
            } else {
                if (skew != 0) {
                    uint64_t bit = ((1llu << tid) xor (uint64_t) (-1));
                    intensive.fetch_and(bit, std::memory_order_relaxed);
                    skew = 0;
                }
            }
        }
        if (skew != 0) {
            readintensive[tid].fetch_add(1);
            holders[tid].store(address);
        } else {
            writeintensive[tid].fetch_add(1);
            hashkey = simplehash(address);
            indicators[hashkey].fetch_add();
        }
        return address;
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, (std::atomic<uint64_t> &) res);
    }

    void read(size_t tid) {
        if (skew != 0) holders[tid].store(0);
        else indicators[hashkey].fetch_sub();
    }

    bool free(uint64_t ptr) {
        uint64_t intention = intensive.load(std::memory_order_relaxed);
        assert(ptr != 0);
        bool busy;
        uint64_t hk = simplehash(ptr);
        busy = (indicators[hk].load() != 0);
        if (busy) return false;
        if (intention > 0) {
            for (size_t t = 0; t < thread_number; t++) {
                if ((intention & (1llu << t)) != 0 && holders[t].load() == ptr) {
                    busy = true;
                    break;
                }
            }
            if (busy) return false;
        }
        std::free((void *) ptr);
        return true;
    }
};

#endif //HASHCOMP_ADAPTIVE_HAZARD_H
