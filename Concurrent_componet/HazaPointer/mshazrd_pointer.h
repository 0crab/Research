//
// Created by Michael on 11/25/2019.
// This class includes the wrapper of MichaelScott's Hazard Point in TPDS04
//

#ifndef HASHCOMP_MSHAZRD_POINTER_H
#define HASHCOMP_MSHAZRD_POINTER_H

#include "ihazard.h"

/******************************************************************************
 * Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Concurrency Freaks nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.

 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************
 */

#include <atomic>
#include <iostream>
#include <functional>
#include <vector>

template<typename T>
class HazardPointers {
private:
    static const int HP_MAX_THREADS = 128;
    static const int HP_MAX_HPS = 128;     // This is named 'K' in the HP paper
    static const int CLPAD = 128 / sizeof(std::atomic<T *>);
    static const int HP_THRESHOLD_R = 0; // This is named 'R' in the HP paper
    static const int MAX_RETIRED = HP_MAX_THREADS * HP_MAX_HPS; // Maximum number of retired objects per thread

    const int maxHPs;
    const int maxThreads;

    alignas(128) std::atomic<T *> *hp[HP_MAX_THREADS];
    // It's not nice that we have a lot of empty vectors, but we need padding to avoid false sharing
    alignas(128) std::vector<T *> retiredList[HP_MAX_THREADS * CLPAD];
    std::function<void(T *, int)> defdeleter = [](T *t, int tid) { delete t; };
    std::function<void(T *, int)> &deleter;
public:

    HazardPointers(int maxHPs, int maxThreads) : maxHPs{maxHPs}, maxThreads{maxThreads}, deleter{defdeleter} {
        for (int ithread = 0; ithread < HP_MAX_THREADS; ithread++) {
            hp[ithread] = new std::atomic<T *>[HP_MAX_HPS];
            for (int ihp = 0; ihp < HP_MAX_HPS; ihp++) {
                hp[ithread][ihp].store(nullptr, std::memory_order_relaxed);
            }
        }
    }

    HazardPointers(int maxHPs, int maxThreads, std::function<void(T *, int)> &deleter) : maxHPs{maxHPs},
                                                                                         maxThreads{maxThreads},
                                                                                         deleter{deleter} {
        for (int ithread = 0; ithread < HP_MAX_THREADS; ithread++) {
            hp[ithread] = new std::atomic<T *>[HP_MAX_HPS];
            for (int ihp = 0; ihp < HP_MAX_HPS; ihp++) {
                hp[ithread][ihp].store(nullptr, std::memory_order_relaxed);
            }
        }
    }

    ~HazardPointers() {
        for (int ithread = 0; ithread < HP_MAX_THREADS; ithread++) {
            delete[] hp[ithread];
            // Clear the current retired nodes
            for (unsigned iret = 0; iret < retiredList[ithread * CLPAD].size(); iret++) {
                delete retiredList[ithread * CLPAD][iret];
            }
        }
    }

    /**
     * Progress Condition: wait-free bounded (by maxHPs)
     */
    void clear(const int tid) {
        for (int ihp = 0; ihp < maxHPs; ihp++) {
            hp[tid][ihp].store(nullptr, std::memory_order_release);
        }
    }

    /**
     * Progress Condition: wait-free population oblivious
     */
    void clearOne(int ihp, const int tid) {
        hp[tid][ihp].store(nullptr, std::memory_order_release);
    }

    /**
     * Progress Condition: lock-free
     */
    T *protect(int index, const std::atomic<T *> &atom, const int tid) {
        T *n = nullptr;
        T *ret;
        while ((ret = atom.load()) != n) {
            hp[tid][index].store(ret);
            n = ret;
        }
        return ret;
    }

    T *get(int index, const int tid) {
        return hp[tid][index].load();
    }

    /**
     * This returns the same value that is passed as ptr, which is sometimes useful
     * Progress Condition: wait-free population oblivious
     */
    T *protectPtr(int index, T *ptr, const int tid) {
        hp[tid][index].store(ptr);
        return ptr;
    }

    /**
     * This returns the same value that is passed as ptr, which is sometimes useful
     * Progress Condition: wait-free population oblivious
     */
    T *protectPtrRelease(int index, T *ptr, const int tid) {
        hp[tid][index].store(ptr, std::memory_order_release);
        return ptr;
    }

    /**
     * Progress Condition: wait-free bounded (by the number of threads squared)
     */
    void retire(T *ptr, const int tid) {
        retiredList[tid * CLPAD].push_back(ptr);
        if (retiredList[tid * CLPAD].size() < HP_THRESHOLD_R) return;
        for (unsigned iret = 0; iret < retiredList[tid * CLPAD].size();) {
            auto obj = retiredList[tid * CLPAD][iret];
            bool canDelete = true;
            for (int tid = 0; tid < maxThreads && canDelete; tid++) {
                for (int ihp = maxHPs - 1; ihp >= 0; ihp--) {
                    if (hp[tid][ihp].load() == obj) {
                        canDelete = false;
                        break;
                    }
                }
            }
            if (canDelete) {
                retiredList[tid * CLPAD].erase(retiredList[tid * CLPAD].begin() + iret);
                deleter(obj, tid);
                continue;
            }
            iret++;
        }
    }
};

thread_local int ftid;

template<class T, class D = T>
class mshazard_pointer : public virtual ihazard<T, D> {
private:
    HazardPointers<uint64_t> *hp;
protected:
    using ihazard<T, D>::thread_number;
public:
    mshazard_pointer(size_t thread_number) {
        hp = new HazardPointers<uint64_t>(2, thread_number);
        std::cout << "MS HazardPointer" << std::endl;
    }

    void registerThread() { ftid = thread_number++; }

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        return (uint64_t) hp->protect(0, (std::atomic<uint64_t *> &) ptr, tid);
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, (std::atomic<uint64_t> &) res);
    }

    void read(size_t tid) { hp->clearOne(0, tid); }

    bool free(uint64_t ptr) { hp->retire((uint64_t *) ptr, ftid); }

    const char *info() { return "mshazardpoint"; }
};

#endif //HASHCOMP_MSHAZRD_POINTER_H
