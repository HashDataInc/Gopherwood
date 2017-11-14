#!/bin/bash

set -x -e

# install packages needed to build and run GPDB
curl -L https://pek3a.qingstor.com/hashdata-public/tools/centos7/libarchive/libarchive.repo -o /etc/yum.repos.d/libarchive.repo
sudo yum -y update
sudo yum -y groupinstall "Development Tools"
sudo yum -y install epel-release
sudo yum -y install \
	vim \
	doxygen \
	cmake \
	which \
	make \
	rpmdevtools \
	gcc-c++ \
	boost-devel \
	cyrus-sasl-devel \
	cyrus-sasl-plain \
	cyrus-sasl-gssapi \
	libevent-devel \
	zlib-devel \
	openssl-devel \
	wget \
	git \
	java-1.8.0-openjdk \
	ed \
	which \
	go \
	json-c-devel \
	curl-devel \
	libyaml-devel \
	protobuf \

#build QingStor-C-SDK

cd ~
git clone https://github.com/jianlirong/QingStor-C-SDK.git
cd QingStor-C-SDK
mkdir build
cd build
../bootstrap --prefix=/usr/local
make
sudo make install

