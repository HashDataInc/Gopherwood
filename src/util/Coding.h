#ifndef _GOPHERWOOD_UTIL_CODING_H_
#define _GOPHERWOOD_UTIL_CODING_H_


#include <algorithm>
#include <stdint.h>
#include <string.h>
#include <string>
#include "Slice.h"
#include "../port/PortPosix.h"

// Some processors does not allow unaligned access to memory
#if defined(__sparc)
#define PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED
#endif

namespace Gopherwood {

// The maximum length of a varint in bytes for 64-bit.
    const unsigned int kMaxVarint64Length = 10;

// Standard Put... routines append to a string
    extern void PutFixed32(std::string *dst, int32_t value);

    extern void PutFixed64(std::string *dst, int64_t value);

    extern void PutVarint32(std::string *dst, int32_t value);

    extern void PutVarint32Varint32(std::string *dst, int32_t value1,
                                    int32_t value2);

    extern void PutVarint32Varint32Varint32(std::string *dst, int32_t value1,
                                            int32_t value2, int32_t value3);

    extern void PutVarint64(std::string *dst, int64_t value);

    extern void PutVarint64Varint64(std::string *dst, int64_t value1,
                                    int64_t value2);

    extern void PutVarint32Varint64(std::string *dst, int32_t value1,
                                    int64_t value2);

    extern void PutVarint32Varint32Varint64(std::string *dst, int32_t value1,
                                            int32_t value2, int64_t value3);

    extern void PutLengthPrefixedSlice(std::string *dst, const Slice &value);

    extern void PutLengthPrefixedSliceParts(std::string *dst,
                                            const SliceParts &slice_parts);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
    extern bool GetFixed64(Slice *input, int64_t *value);

    extern bool GetFixed32(Slice *input, int32_t *value);

    extern bool GetVarint32(Slice *input, int32_t *value);

    extern bool GetVarint64(Slice *input, int64_t *value);

    extern bool GetLengthPrefixedSlice(Slice *input, Slice *result);

// This function assumes data is well-formed.
    extern Slice GetLengthPrefixedSlice(const char *data);

    extern Slice GetSliceUntil(Slice *slice, char delimiter);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
    extern const char *GetVarint32Ptr(const char *p, const char *limit, int32_t *v);

    extern const char *GetVarint64Ptr(const char *p, const char *limit, int64_t *v);

// Returns the length of the varint32 or varint64 encoding of "v"
    extern int VarintLength(int64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
    extern void EncodeFixed32(char *dst, int32_t value);

    extern void EncodeFixed64(char *dst, int64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
    extern char *EncodeVarint32(char *dst, int32_t value);

    extern char *EncodeVarint64(char *dst, int64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

    inline int32_t DecodeFixed32(const char *ptr) {
        if (port::kLittleEndian) {
            // Load the raw bytes
            int32_t result;
            memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
            return result;
        } else {
            return ((static_cast<int32_t>(static_cast<unsigned char>(ptr[0])))
                    | (static_cast<int32_t>(static_cast<unsigned char>(ptr[1])) << 8)
                    | (static_cast<int32_t>(static_cast<unsigned char>(ptr[2])) << 16)
                    | (static_cast<int32_t>(static_cast<unsigned char>(ptr[3])) << 24));
        }
    }

    inline int64_t DecodeFixed64(const char *ptr) {
        if (port::kLittleEndian) {
            // Load the raw bytes
            int64_t result;
            memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
            return result;
        } else {
            int64_t lo = DecodeFixed32(ptr);
            int64_t hi = DecodeFixed32(ptr + 4);
            return (hi << 32) | lo;
        }
    }

// Internal routine for use by fallback path of GetVarint32Ptr
    extern const char *GetVarint32PtrFallback(const char *p,
                                              const char *limit,
                                              int32_t *value);

    inline const char *GetVarint32Ptr(const char *p,
                                      const char *limit,
                                      int32_t *value) {
        if (p < limit) {
            int32_t result = *(reinterpret_cast<const unsigned char *>(p));
            if ((result & 128) == 0) {
                *value = result;
                return p + 1;
            }
        }
        return GetVarint32PtrFallback(p, limit, value);
    }

// -- Implementation of the functions declared above
    inline void EncodeFixed32(char *buf, int32_t value) {
        if (port::kLittleEndian) {
            memcpy(buf, &value, sizeof(value));
        } else {
            buf[0] = value & 0xff;
            buf[1] = (value >> 8) & 0xff;
            buf[2] = (value >> 16) & 0xff;
            buf[3] = (value >> 24) & 0xff;
        }
    }

    inline void EncodeFixed64(char *buf, int64_t value) {
        if (port::kLittleEndian) {
            memcpy(buf, &value, sizeof(value));
        } else {
            buf[0] = value & 0xff;
            buf[1] = (value >> 8) & 0xff;
            buf[2] = (value >> 16) & 0xff;
            buf[3] = (value >> 24) & 0xff;
            buf[4] = (value >> 32) & 0xff;
            buf[5] = (value >> 40) & 0xff;
            buf[6] = (value >> 48) & 0xff;
            buf[7] = (value >> 56) & 0xff;
        }
    }

// Pull the last 8 bits and cast it to a character
    inline void PutFixed32(std::string *dst, int32_t value) {
        if (port::kLittleEndian) {
            dst->append(const_cast<const char *>(reinterpret_cast<char *>(&value)),
                        sizeof(value));
        } else {
            char buf[sizeof(value)];
            EncodeFixed32(buf, value);
            dst->append(buf, sizeof(buf));
        }
    }

    inline void PutFixed64(std::string *dst, int64_t value) {
        if (port::kLittleEndian) {
            dst->append(const_cast<const char *>(reinterpret_cast<char *>(&value)),
                        sizeof(value));
        } else {
            char buf[sizeof(value)];
            EncodeFixed64(buf, value);
            dst->append(buf, sizeof(buf));
        }
    }

    inline void PutVarint32(std::string *dst, int32_t v) {
        char buf[5];
        char *ptr = EncodeVarint32(buf, v);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutVarint32Varint32(std::string *dst, int32_t v1, int32_t v2) {
        char buf[10];
        char *ptr = EncodeVarint32(buf, v1);
        ptr = EncodeVarint32(ptr, v2);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutVarint32Varint32Varint32(std::string *dst, int32_t v1,
                                            int32_t v2, int32_t v3) {
        char buf[15];
        char *ptr = EncodeVarint32(buf, v1);
        ptr = EncodeVarint32(ptr, v2);
        ptr = EncodeVarint32(ptr, v3);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline char *EncodeVarint64(char *dst, int64_t v) {
        static const unsigned int B = 128;
        unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
        while (v >= B) {
            *(ptr++) = (v & (B - 1)) | B;
            v >>= 7;
        }
        *(ptr++) = static_cast<unsigned char>(v);
        return reinterpret_cast<char *>(ptr);
    }

    inline void PutVarint64(std::string *dst, int64_t v) {
        char buf[10];
        char *ptr = EncodeVarint64(buf, v);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutVarint64Varint64(std::string *dst, int64_t v1, int64_t v2) {
        char buf[20];
        char *ptr = EncodeVarint64(buf, v1);
        ptr = EncodeVarint64(ptr, v2);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutVarint32Varint64(std::string *dst, int32_t v1, int64_t v2) {
        char buf[15];
        char *ptr = EncodeVarint32(buf, v1);
        ptr = EncodeVarint64(ptr, v2);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutVarint32Varint32Varint64(std::string *dst, int32_t v1,
                                            int32_t v2, int64_t v3) {
        char buf[20];
        char *ptr = EncodeVarint32(buf, v1);
        ptr = EncodeVarint32(ptr, v2);
        ptr = EncodeVarint64(ptr, v3);
        dst->append(buf, static_cast<size_t>(ptr - buf));
    }

    inline void PutLengthPrefixedSlice(std::string *dst, const Slice &value) {
        PutVarint32(dst, static_cast<int32_t>(value.size()));
        dst->append(value.data(), value.size());
    }

    inline void PutLengthPrefixedSliceParts(std::string *dst,
                                            const SliceParts &slice_parts) {
        size_t total_bytes = 0;
        for (int i = 0; i < slice_parts.num_parts; ++i) {
            total_bytes += slice_parts.parts[i].size();
        }
        PutVarint32(dst, static_cast<int32_t>(total_bytes));
        for (int i = 0; i < slice_parts.num_parts; ++i) {
            dst->append(slice_parts.parts[i].data(), slice_parts.parts[i].size());
        }
    }

    inline int VarintLength(int64_t v) {
        int len = 1;
        while (v >= 128) {
            v >>= 7;
            len++;
        }
        return len;
    }

    inline bool GetFixed64(Slice *input, int64_t *value) {
        if (input->size() < sizeof(int64_t)) {
            return false;
        }
        *value = DecodeFixed64(input->data());
        input->remove_prefix(sizeof(int64_t));
        return true;
    }

    inline bool GetFixed32(Slice *input, int32_t *value) {
        if (input->size() < sizeof(int32_t)) {
            return false;
        }
        *value = DecodeFixed32(input->data());
        input->remove_prefix(sizeof(int32_t));
        return true;
    }

    inline bool GetVarint32(Slice *input, int32_t *value) {
        const char *p = input->data();
        const char *limit = p + input->size();
        const char *q = GetVarint32Ptr(p, limit, value);
        if (q == nullptr) {
            return false;
        } else {
            *input = Slice(q, static_cast<size_t>(limit - q));
            return true;
        }
    }

    inline bool GetVarint64(Slice *input, int64_t *value) {
        const char *p = input->data();
        const char *limit = p + input->size();
        const char *q = GetVarint64Ptr(p, limit, value);
        if (q == nullptr) {
            return false;
        } else {
            *input = Slice(q, static_cast<size_t>(limit - q));
            return true;
        }
    }

// Provide an interface for platform independent endianness transformation
    inline int64_t EndianTransform(int64_t input, size_t size) {
        char *pos = reinterpret_cast<char *>(&input);
        int64_t ret_val = 0;
        for (size_t i = 0; i < size; ++i) {
            ret_val |= (static_cast<int64_t>(static_cast<unsigned char>(pos[i]))
                    << ((size - i - 1) << 3));
        }
        return ret_val;
    }

    inline bool GetLengthPrefixedSlice(Slice *input, Slice *result) {
        int32_t len = 0;
        if (GetVarint32(input, &len) && input->size() >= (size_t)len) {
            *result = Slice(input->data(), len);
            input->remove_prefix(len);
            return true;
        } else {
            return false;
        }
    }

    inline Slice GetLengthPrefixedSlice(const char *data) {
        int32_t len = 0;
        // +5: we assume "data" is not corrupted
        // unsigned char is 7 bits, int32_t is 32 bits, need 5 unsigned char
        auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
        return Slice(p, len);
    }

    inline Slice GetSliceUntil(Slice *slice, char delimiter) {
        int32_t len = 0;
        for (len = 0; (size_t)len < slice->size() && slice->data()[len] != delimiter; ++len) {
            // nothing
        }

        Slice ret(slice->data(), len);
        slice->remove_prefix(len + (((size_t)len < slice->size()) ? 1 : 0));
        return ret;
    }

    template<class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
    __attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
    __attribute__((__no_sanitize_undefined__))
#endif
#endif
    inline void PutUnaligned(T *memory, const T &value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
        char *nonAlignedMemory = reinterpret_cast<char*>(memory);
        memcpy(nonAlignedMemory, reinterpret_cast<const char*>(&value), sizeof(T));
#else
        *memory = value;
#endif
    }

    template<class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
    __attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
    __attribute__((__no_sanitize_undefined__))
#endif
#endif
    inline void GetUnaligned(const T *memory, T *value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
        char *nonAlignedMemory = reinterpret_cast<char*>(value);
        memcpy(nonAlignedMemory, reinterpret_cast<const char*>(memory), sizeof(T));
#else
        *value = *memory;
#endif
    }

}

#endif
