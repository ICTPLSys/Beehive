#!/bin/bash

export LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:$LD_LIBRARY_PATH
export PATH="/usr/lib/ccache:$PATH"
export LIBFIBRE_DIR=/path/to/libfibre/

cd build/
cmake ..
make -j32