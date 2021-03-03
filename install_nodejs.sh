#!/bin/bash
# 安装nodejs

wget https://nodejs.org/dist/v8.11.4/node-v8.11.4.tar.gz
./configure --prefix=/usr/local/node; make install
export EXECJS_RUNTIME=/usr/local/node