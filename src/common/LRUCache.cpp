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
#ifndef _GOPHERWOOD_COMMON_LRUCACHE_H_
#define _GOPHERWOOD_COMMON_LRUCACHE_H_

#include <unordered_map>
#include <list>
#include <vector>
#include <assert.h>
#include "Logger.h"

namespace Gopherwood {
namespace Internal {

template<typename key_t, typename value_t>
class LRUCache {
public:
    typedef typename std::pair<key_t, value_t> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

    LRUCache(size_t max_size) :
            _max_size(max_size) {
    }

    std::vector<key_t> adjustSize(size_t newSize) {
        assert(newSize > 0);
        std::vector<key_t> removeVector;
        size_t currSize = size();
        if (newSize < currSize) {
            size_t numToRemove = currSize - newSize;
            removeVector = removeNumOfKeys(numToRemove);
        } else {
            removeVector = NULL;
        }
        this->_max_size = newSize;
        return removeVector;
    }

    std::vector<key_t> put(const key_t &key, const value_t &value) {
        auto it = _cache_items_map.find(key);
        _cache_items_list.push_front(key_value_pair_t(key, value));
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
        _cache_items_map[key] = _cache_items_list.begin();

        std::vector<key_t> deleteVector;

        if (_cache_items_map.size() > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
            deleteVector.push_back(last->first);
        }
        return deleteVector;
    }

    std::vector<key_t> removeNumOfKeys(size_t num) {
        std::vector<key_t> removeVector;
        size_t removeNum = num > _cache_items_map.size() ? _cache_items_map.size() : num;
        while (removeNum > 0) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
            removeVector.push_back(last->first);
            removeNum--;
        }
        return removeVector;
    }

    void deleteObject(const key_t &key) {
        auto it = _cache_items_map.find(key);
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
    }

    std::vector<key_t> getAllKeyObject() {
        std::vector<key_t> allKeyVector;
        for (key_value_pair_t tmpKV : _cache_items_list) {
            allKeyVector.push_back(tmpKV.first);
        }
        return allKeyVector;
    }

    const bool get(const key_t &key) {
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            LOG(Gopherwood::Internal::INFO, "There is no such key in cache");
            return false;
        } else {
            _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
            return true;
//                return it->second->second;
        }
    }

    bool exists(const key_t &key) const {
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() const {
        return _cache_items_map.size();
    }

    void printLruCache() {
        LOG(Gopherwood::Internal::INFO, "start to print the lru cache status");
        for (key_value_pair_t tmpKV : _cache_items_list) {
            LOG(
                    Gopherwood::Internal::INFO,
                    "the key = %d, the value =%d",
                    tmpKV.first,
                    tmpKV.second);
        }
        LOG(Gopherwood::Internal::INFO, "the end of print the lru cache status");
    }

private:
    std::list<key_value_pair_t> _cache_items_list;
    std::unordered_map<key_t, list_iterator_t> _cache_items_map;
    size_t _max_size;
};

}
} // namespace Gopherwood

#endif    /* _GOPHERWOOD_COMMON_LRUCACHE_H_ */
