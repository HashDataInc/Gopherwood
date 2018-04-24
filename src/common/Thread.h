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
#ifndef _GOPHERWOOD_COMMON_THREAD_H_
#define _GOPHERWOOD_COMMON_THREAD_H_

#include "platform.h"

#include <signal.h>

#ifdef NEED_BOOST

#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/thread/future.hpp>
#include <boost/utility/result_of.hpp>
#include <boost/move/utility.hpp>

namespace Gopherwood {
namespace Internal {

using boost::thread;
using boost::bind;
using boost::mutex;
using boost::lock_guard;
using boost::unique_lock;
using boost::condition_variable;
using boost::defer_lock_t;
using boost::once_flag;
using boost::call_once;
using boost::function;
using boost::result_of;
using boost::forward;
using boost::chrono::seconds;
using boost::unique_future;
using boost::packaged_task;
using namespace boost::this_thread;
}
}

#else

#include <thread>
#include <mutex>
#include <future>
#include <functional>
#include <condition_variable>

namespace Gopherwood {
    namespace Internal {

        using std::thread;
        using std::bind;
        using std::mutex;
        using std::lock_guard;
        using std::future;
        using std::unique_lock;
        using std::condition_variable;
        using std::defer_lock_t;
        using std::once_flag;
        using std::call_once;
        using std::function;
        using std::result_of;
        using std::forward;
        using std::packaged_task;
        using std::chrono::seconds;
        using namespace std::this_thread;

    }
}
#endif

namespace Gopherwood {
namespace Internal {

/*
 * make the background thread ignore these signals (which should allow that
 * they be delivered to the main thread)
 */
sigset_t ThreadBlockSignal();

/*
 * Restore previous signals.
 */
void ThreadUnBlockSignal(sigset_t sigs);

}
}

#define CREATE_THREAD(retval, fun) \
    do { \
        sigset_t sigs = Gopherwood::Internal::ThreadBlockSignal(); \
        try { \
            retval = Gopherwood::Internal::thread(fun); \
            Gopherwood::Internal::ThreadUnBlockSignal(sigs); \
        } catch (...) { \
        	Gopherwood::Internal::ThreadUnBlockSignal(sigs); \
            throw; \
        } \
    } while(0)

#endif /* _GOPHERWOOD_COMMON_THREAD_H_ */
