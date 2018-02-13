#ifndef _GOPHERWOOD_UTIL_CLEANABLE_H_
#define _GOPHERWOOD_UTIL_CLEANABLE_H_

namespace Gopherwood {

class Cleanable {
public:
    Cleanable();

    ~Cleanable();

    // No copy constructor and copy assignment allowed.
    Cleanable(Cleanable &) = delete;

    Cleanable &operator=(Cleanable &) = delete;

    // Move consturctor and move assignment is allowed.
    Cleanable(Cleanable &&);

    Cleanable &operator=(Cleanable &&);

    // Clients are allowed to register function/arg1/arg2 triples that
    // will be invoked when this iterator is destroyed.
    //
    // Note that unlike all of the preceding methods, this method is
    // not abstract and therefore clients should not override it.
    typedef void (*CleanupFunction)(void *arg1, void *arg2);

    void RegisterCleanup(CleanupFunction function, void *arg1, void *arg2);

    void DelegateCleanupsTo(Cleanable *other);

    // DoCleanup and also resets the pointers for reuse
    inline void Reset() {
        DoCleanup();
        cleanup_.function = nullptr;
        cleanup_.next = nullptr;
    }

protected:
    struct Cleanup {
        CleanupFunction function;
        void *arg1;
        void *arg2;
        Cleanup *next;
    };
    Cleanup cleanup_;

    // It also becomes the owner of c
    void RegisterCleanup(Cleanup *c);

private:
    // Performs all the cleanups. It does not reset the pointers. Making it
    // private
    // to prevent misuse
    inline void DoCleanup() {
        if (cleanup_.function != nullptr) {
            (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
            for (Cleanup *c = cleanup_.next; c != nullptr;) {
                (*c->function)(c->arg1, c->arg2);
                Cleanup *next = c->next;
                delete c;
                c = next;
            }
        }
    }
};

}

#endif  // _GOPHERWOOD_UTIL_CLEANABLE_H_
