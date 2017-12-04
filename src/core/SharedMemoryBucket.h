////
//// Created by root on 11/20/17.
////
//#include <vector>
//
//#include "FSConfig.h"
//
//
//#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYBUCKET_H_
//#define _GOPHERWOOD_CORE_SHAREDMEMORYBUCKET_H_
//
//
//namespace Gopherwood {
//
//    using namespace std;
//
//    using namespace Internal;
//
//    class SharedMemoryBucket {
//
//
//    private:
//        //TODO
//
////        static const int32_t BIT_MAP_SIZE = 40;
//        vector<int> bmvector;
//
//    public:
//
//        SharedMemoryBucket(){
//            bmvector.reserve(BIT_MAP_SIZE);
//        }
//        ~SharedMemoryBucket(){
//
//        }
//        vector<int> &getBmvector() {
//            return bmvector;
//        }
//
//        void setBmvector(vector<int>& bmvector) {
//            SharedMemoryBucket::bmvector = bmvector;
//        }
//    };
//}
//
//#endif //_GOPHERWOOD_CORE_SHAREDMEMORYBUCKET_H_