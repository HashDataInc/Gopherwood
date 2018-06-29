1.在官网上(http://www.tpc.org/tpcds/ )去下载最新的：TPC-DS. 

2.解压: 下载的 zip 文件放在 linux 上解压,并进入他的 tools 目录.

3.编译：make  这里需要linux安装上了 gcc , gcc c++, expect 等.

4.生成数据：在tools目录下执行：./dsdgen -scale 5 -force (-force:会覆盖原来生成的data,否则不覆盖);生成的25个.dat 的数据文件.
  -scale代表生成多大的数据
