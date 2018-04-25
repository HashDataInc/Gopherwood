#ifndef _GOPHERWOOD_COMMON_THREAD_POOL_H
#define _GOPHERWOOD_COMMON_THREAD_POOL_H

#include "common/Thread.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include <vector>
#include <queue>

namespace Gopherwood {
namespace Internal {

class ThreadPool {
public:
    ThreadPool(size_t);

    template<class F, class... Args>
    auto enqueue(F &&f, Args &&... args) -> future<typename result_of<F(Args...)>::type>;

    ~ThreadPool();

private:
    // need to keep track of threads so we can join them
    std::vector<thread> workers;
    // the task queue
    std::queue<function<void()> > tasks;

    // synchronization
    mutex queue_mutex;
    condition_variable condition;
    bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
        : stop(false) {
    sigset_t sigs = ThreadBlockSignal();
    try {
        for (size_t i = 0; i < threads; ++i)
            workers.emplace_back(
                    [this] {
                        for (;;) {
                            function<void()> task;

                            {
                                unique_lock<mutex> lock(this->queue_mutex);
                                this->condition.wait(lock,
                                                     [this] { return this->stop || !this->tasks.empty(); });
                                if (this->stop && this->tasks.empty())
                                    return;
                                task = move(this->tasks.front());
                                this->tasks.pop();
                            }

                            task();
                        }
                    }
            );
        ThreadUnBlockSignal(sigs);
    } catch (...) {
        ThreadUnBlockSignal(sigs);
        throw;
    }
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
-> future<typename result_of<F(Args...)>::type> {
    using return_type = typename result_of<F(Args...)>::type;

    auto task = make_shared<packaged_task<return_type()> >(
            bind(forward<F>(f), forward<Args>(args)...)
    );

    future<return_type> res = task->get_future();
    {
        unique_lock<mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if (stop)
            THROW(GopherwoodIOException,
                  "[ThreadPool::enqueue] enqueuing on stopped ThreadPool");

        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
    {
        unique_lock<mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (thread &worker: workers)
        worker.join();
}

}
}
#endif