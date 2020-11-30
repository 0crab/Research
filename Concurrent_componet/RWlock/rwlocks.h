//#define WRITERS_FAVOR_RWLOCK
#define READERS_FAVOR_RWLOCK
//#define PTHREAD_RWLOCK


#ifdef WRITERS_FAVOR_RWLOCK
class alignas(128) Lock  {
public:
    Lock() : rwlock(0){}
    Lock(const Lock & other): rwlock(0){}

    inline bool isWriteLocked() {
        return rwlock & 1;
    }
    inline bool isReadLocked() {
        return rwlock & ~1;
    }

    inline bool isLocked() {
        return rwlock;
    }

    void wLock() noexcept {
        //printf("%lu try lock write %lu:%lld\n",pthread_self()%1000,(uint64_t)(&rwlock)%1000,rwlock);
        while(isWriteLocked());
        while (1) {
            long long v = rwlock;
            if (__sync_bool_compare_and_swap(&rwlock, v & ~1, v | 1)) {
                while (v & ~1) { // while there are still readers
                    v = rwlock;
                }
                return;
            }
        }
    }

    void rLock(){
        //printf("--------------------------%lu try lock read %lu:%lld\n",pthread_self()%1000,(uint64_t)(&rwlock)%1000,rwlock);
        while (1) {
            while (isWriteLocked()) {}
            if ((__sync_add_and_fetch(&rwlock, 2) & 1) == 0) return; // when we tentatively read-locked, there was no writer
            __sync_add_and_fetch(&rwlock, -2); // release our tentative read-lock
        }
    }

    void rLock_1(){
        //printf("--------------------------%lu try lock read %lu:%lld\n",pthread_self()%1000,(uint64_t)(&rwlock)%1000,rwlock);
        while (1) {
            while (isWriteLocked()) {}
            if ((__sync_add_and_fetch(&rwlock, 2) & 1) == 0) return; // when we tentatively read-locked, there was no writer
            __sync_add_and_fetch(&rwlock, -2); // release our tentative read-lock
        }
    }

    inline bool try_upgradeLock() {
        long long v = rwlock;
        if (__sync_bool_compare_and_swap(&rwlock, v & ~1, v | 1)) {
            __sync_add_and_fetch(&rwlock, -2);
            while (v & ~1) { // while there are still readers
                v = rwlock;
            }
            return true;
        }
        return false;
    }

    inline void degradeLock(){
        __sync_add_and_fetch(&rwlock,1);
    }

    void rUnlock() noexcept{
        __sync_add_and_fetch(&rwlock, -2);
    }

    void wUnlock() noexcept{
        __sync_add_and_fetch(&rwlock, -1);
    }

//private:
    volatile long long rwlock;
};
#endif

#ifdef READERS_FAVOR_RWLOCK
class alignas(128) Lock  {
public:
    Lock() : rwlock(0){}
    Lock(const Lock & other): rwlock(0){}

    inline bool isWriteLocked() {
        return rwlock & 1;
    }
    inline bool isReadLocked() {
        return rwlock & ~3;
    }

    inline bool isUpgraeding(){
        return rwlock & 2;
    }

    inline bool isLocked() {
        return rwlock;
    }

    void wLock() noexcept {
        while (1) {
            while (isLocked()) {}
            if (__sync_bool_compare_and_swap(&rwlock, 0, 1)) {
                return;
            }
        }
    }

    void wUnlock() noexcept{
        __sync_add_and_fetch(&rwlock, -1);
    }

    void rLock(){
        __sync_add_and_fetch(&rwlock, 4);
        while (isWriteLocked());
        return;
    }

    void rLock_1(){
        while(isUpgraeding());
        __sync_add_and_fetch(&rwlock, 4);
        while (isWriteLocked());
        return;
    }

    inline bool try_upgradeLock() {
        while (1) {
            auto expval = rwlock;
            if (expval & 2) return false;
            auto seenval = __sync_val_compare_and_swap(&rwlock, expval, (expval - 4) |
                                                                      2 /* subtract our reader count and covert to upgrader */);
            if (seenval == expval) { // cas success
                // cas to writer
                while (1) {
                    while (rwlock & ~2 /* locked by someone else */) {}
                    if (__sync_bool_compare_and_swap(&rwlock, 2, 1)) {
                        return true;
                    }
                }
            }
        }
    }

    void degradeLock(){
        __sync_fetch_and_add(&rwlock, 3);
    }

    void rUnlock() noexcept{
        __sync_add_and_fetch(&rwlock, -4);
    }

//private:
    volatile long long rwlock;
};
#endif

#ifdef PTHREAD_RWLOCK
class alignas(128) Lock  {
public:
    Lock()  {
        pthread_rwlock_init(&rwlock, NULL) ;
    }

    Lock(const Lock & other) {
        pthread_rwlock_init(&rwlock, NULL) ;
    }

    inline void rLock() {
        pthread_rwlock_rdlock(&rwlock) ;
    }
    inline void rUnlock() {
        pthread_rwlock_unlock(&rwlock) ;
    }
    inline void wLock() {
        pthread_rwlock_wrlock(&rwlock) ;
    }
    inline void wUnlock() {
        pthread_rwlock_unlock(&rwlock) ;
    }

//private:
    pthread_rwlock_t rwlock;
};
#endif

