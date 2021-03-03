#!/bin/bash
# 安装nodejs

wget https://nodejs.org/dist/v8.11.4/node-v8.11.4.tar.gz
tar -xzvf node-v8.11.4.tar.gz
cd node-v8.11.4
./configure --prefix=/usr/local/node; make install
export EXECJS_RUNTIME=/usr/local/node