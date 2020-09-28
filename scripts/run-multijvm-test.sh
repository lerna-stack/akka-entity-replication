#!/usr/bin/bash

# Usage
# ./run-multijvm-test.sh
# 
# You can specify max attempts(retry).
# ./run-multijvm-test.sh 10

# Set MaxRetry
MAX_RETRY=3
if [ $# -ge 1 ]; then
    MAX_RETRY=$1
fi

# Run multi-jvm:test until it is success or it's attempts reach to MAX_RETRY.
code=0
for i in `seq 1 $MAX_RETRY`; do
    echo "begin ($i/$MAX_RETRY)"
    sbt multi-jvm:test
    code=$?
    if [ $code -eq 0 ]; then
        break
    fi
    if [ $i -ne $MAX_RETRY ]; then
        echo "fail, and then try it again..."
        sleep 1
    fi
done

exit $code