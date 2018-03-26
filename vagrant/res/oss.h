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

#ifndef __OSS_LIBOSS_OSS_H_
#define __OSS_LIBOSS_OSS_H_

#include <stdint.h>

#ifndef EINTERNAL
#define EINTERNAL 255
#endif

/** All APIs set errno to meaningful values */
#ifdef __cplusplus
extern "C" {
#endif

struct ContextInternalWrapper;
typedef struct ContextInternalWrapper *ossContext;

struct OSSObjectInternalWrapper;
typedef struct OSSObjectInternalWrapper *ossObject;

/**
 * ossBucketInfo - Information about a OSS bucket.
 */
typedef struct {
	char *name;
	char *created;
} ossBucketInfo;

typedef struct
{
	int64_t start;
	int64_t end;
} ossRangeInfo;

/**
 * ossObjectInfo - Information about a OSS object.
 */
typedef struct
{
	char *key;
	int64_t size;
	ossRangeInfo range;
} ossObjectInfo;

/**
 * ossListObjectResult - Result of ListObject
 */
typedef struct
{
	char *name;
	char *prefix;
	unsigned int limit;
	ossObjectInfo *objects;
	int nObjects;
} ossObjectResult;

/**
 * ossHeadResult - Result of ossHeadObject
 */
typedef struct
{
	char *content_type;
	int64_t content_length;
	char *last_modified;
	char *etag;
} ossHeadResult;

/**
 * Return error information of last failed operation.
 *
 * @return			A non NULL constant string pointer of last error information.
 * 					Caller can only read this message and keep it unchanged. No need
 * 					to free it. If last operation finished successfully, the returned
 * 					message is undefined.
 */
const char * ossGetLastError();

/**
 * ossInitContext - Initialize the OSS Context.
 *
 * @param OSSType				The OSS platform : KS3, QS, ALI, COS.
 * @param location				The access location for accessing OSS.
 * @param appid					The application ID.
 * @param access_key_id			The access key id for accessing OSS.
 * @param secret_access_key		The secret access key for accessing OSS.
 * @param write_chunk_size		The write buffer size of OSS.
 * @param read_chunk_size		The read buffer size of OSS.
 * @return						Returns a handle to the OSS Context, or NULL on error.
 */
ossContext ossInitContext(const char *OSSType, const char *location, const char *appid, const char *access_key_id, const char *secret_access_key, int64_t write_chunk_size, int64_t read_chunk_size);

/**
 * ossInitContextFromFile - Initialize the OSS Context through a configuration file.
 *
 * @param config_file			The configuration file for initializing the OSS Context.
 * @return						Returns a handle to the OSS Context, or NULL on error.
 */
ossContext ossInitContextFromFile(const char *OSSType, const char *config_file);

/**
 * ossDestroyContext - Destroy the OSS Context.
 *
 * @param context				The context to be destroyed.
 */
void ossDestroyContext(ossContext context);

/**
 * ossListBuckets - Get list of buckets given a location.
 *
 * @param location				The location of buckets, currently only supporting
 * @param count					Set to the number of buckets.
 * @return						Returns a dynamically-allocated array of ossBucketInfo.
 */
ossBucketInfo* ossListBuckets(ossContext context, const char *location, int *count);

/**
 * ossCreateBucket - Create a bucket given a location
 *
 * @param location			The location of buckets
 * @param bucket				The name of the bucket.
 * @return					Return 0 on success, -1 on error.
 */
int ossCreateBucket(ossContext context, const char *location, const char *bucket);

/**
 * ossDeleteBucket - Delete a bucket given a location and bucket name
 *
 * @param location			The location of buckets
 * @param bucket				The name of the bucket.
 * @return					Return 0 on success, -1 on error.
 */
int ossDeleteBucket(ossContext context, const char *location, const char *bucket);

/**
 * ossListObjects - Get list of objects given a bucket
 *
 * @param bucket				The name of the targeted bucket.
 * @param path				The targeted path.
 * @return					Returns a dynamically-allocated ossListObjectResult.
 */
ossObjectResult* ossListObjects(ossContext context, const char *bucket,
									const char *path);

/**
 * ossGetObject - open a object for read
 *
 * @param bucket				The name of the targeted bucket.
 * @param key				The key of the targeted object.
 * @return					An object handler if exists; otherwise NULL.
 */
ossObject ossGetObject(ossContext context, const char *bucket, const char *key,
						int64_t range_start, int64_t range_end);

/**
 * ossHeadObject - get the information of an object
 *
 * @param bucket					The name of the targeted bucket.
 * @param key					The key of the targeted object.
 * @return						Returns a dynamically-allocated ossHeadResult.
 */
ossHeadResult* ossHeadObject(ossContext context, const char *bucket, const char *key);

/**
 * ossMoveObject - move an object from one bucket to another
 *
 * @param srcBucket				The name of the source bucket.
 * @param srcKey					The name of the source object.
 * @param trgBucket				The name of the target bucket.
 * @param trgKey					The name of the target object.
 * @return						Success or not.
 */
int ossMoveObject(ossContext context, const char *srcBucket, const char *srcKey,
									const char *trgBucket, const char *trgKey);

/**
 * ossPutObject - create a new object for write
 *
 * @param bucket					The name of the targeted bucket.
 * @param key					The key of the targeted object.
 * @param cache                 if cache is true, will be cached first;
 * 								otherwise, will be written immediately
 * return						An object handler if a new object created successfully;
 * 								Otherwise NULL.
 */
ossObject ossPutObject(ossContext context, const char *bucket, const char *key, bool cache);

/**
 * ossCancelObject - cancel an object created for write
 *
 * @param object - 				The targeted object to be canceled.
 * @return						Return true on success, false on error.
 * 								On error, errno will be set appropriately.
 */
int ossCancelObject(ossContext context, ossObject object);

/**
 * ossCloseObject - close an object created for read or write
 *
 * @param object					The targeted object to be closed.
 * @return						Return 0 on success, -1 on error.
 * 								On error, errno will be set appropriately. If the object
 * 								is valid, the memory associated with it will be freed at
 * 								the end of this call, even if there was an I/O error.
 */
int ossCloseObject(ossContext context, ossObject object);

/**
 * ossDeleteObject - delete an object
 *
 * @param bucket					The name of the targeted bucket.
 * @param key					The key of the targeted object.
 * @return						Return 0 on success, -1 on error.
 */
int ossDeleteObject(ossContext context, const char *bucket, const char *key);

/**
 * ossRead - read data from an open object
 *
 * @param object					The targeted object gained by calling ossGetObject.
 * @param buffer					The buffer to copy read bytes into.
 * @param length					The size of the buffer.
 * @return						On success, a positive number indicating how many bytes were read.
 * 								On end-of-file, 0.
 * 								On error, -1. errno will be set to the error code.
 */
int32_t ossRead(ossContext context, ossObject object, void *buffer, int32_t length);

/**
 * ossWrite - write data to an open object
 *
 * @param object					The targeted object gained by calling ossPutObject.
 * @param buffer					The buffer of data to write out.
 * @param length					The size of the buffer.
 * @return						Returns the number of bytes written, -1 on error.
 */
int32_t ossWrite(ossContext context, ossObject object, const char *buffer, int32_t length);

#ifdef __cplusplus
}
#endif
#endif  /* __OSS_LIBOSS_OSS_H_ */

