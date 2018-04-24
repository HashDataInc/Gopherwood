/********************************************************************
 * 2017 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/Memory.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include "common/ThreadPool.h"

namespace Gopherwood {
namespace Internal {

// the constructor just launches some amount of workers
ThreadPool::ThreadPool(size_t threads)
        : stop(false) {

    sigset_t sigs = ThreadBlockSignal();
    try {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back(
                    thread(bind(&ThreadPool::routine, this))
            );
        }
        ThreadUnBlockSignal(sigs);
    } catch (...) {
        ThreadUnBlockSignal(sigs);
        throw;
    }
}

void ThreadPool::routine() {
    try {
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
    } catch (...) {
        LOG(LOG_ERROR, "[ThreadPool]             |"
                "Thread exit with error.");
    }
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
-> unique_future<typename result_of<F(Args...)>::type> {
    using return_type = typename result_of<F(Args...)>::type;

    auto task = make_shared<packaged_task<return_type()> >(
            bind(forward<F>(f), forward<Args>(args)...)
    );

    unique_future<return_type> res = task->get_future();
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
ThreadPool::~ThreadPool() {
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