## Gopherwood

Gopherwood is an embedded persistent caching library with infinite space by leveraging object storage. It
provides unified filesystem APIs for local caching files and supports offloading data to Object
Store Service transparently when caching size exceeded the local disk volume. A block-based metadata
system is designed to support infinite caching space, multi-process accessing, data persistent and
caching recovery.

The project is created to help on-premise system easily transform to cloud-oriented system. Using object   
store service will help solve the data high availability issues and lower the disk cost by configuring a 
minimum local disk quota.

See the [github wiki](https://github.com/neuyilan/Gopherwood/wiki) for detailed documentations, developer guides and FAQs.

## Installation
### Requirement
To build Gopherwood, the following libraries are needed.
    
    cmake (2.8+)                    http://www.cmake.org/
    boost (tested on 1.53+)         http://www.boost.org/
    liboss                          binary integrated in project, will open source later
To run tests, the following libraries are needed.
    
    gtest (tested on 1.7.0)         already integrated in the source code
    gmock (tested on 1.7.0)         already integrated in the source code
    
To run code coverage test, the following tools are needed.

    gcov (included in gcc distribution)
    lcov (tested on 1.9)            http://ltp.sourceforge.net/coverage/lcov.php
### Configuration, Build and Install
    cd GOPHERWOOD_HOME
    mkdir build
    cd build
    ../bootstrap
Run command "../bootstrap --help" for more configuration.
    
    make 
    make install 
    
### Test
To do unit test, run command

    make unittest
    
To do function test, run command

    make functiontest
