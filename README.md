# ssdkv设计思想

## 1.以往工作基础-基于高速非易失存储的缓存管理
为了解决像[Alluxio](http://www.alluxio.org/)这种计算存储分离框架只支持大文件块的读写，不支持小文件块随机读写的问题。借鉴了[memcached](https://memcached.org/)以及[HBase](https://hbase.apache.org/)的内存管理方式，设计了如下的更加细粒度的数据访问接口。支持基于高速非易失存储的内存扩展。
