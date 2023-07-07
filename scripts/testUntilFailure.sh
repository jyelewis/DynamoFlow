#!/bin/sh

for (( i=1; i<100; i++ ))
do
  echo "Running test $i"
  pnpm run test --bail || exit 1
done
echo "Seems pretty stable!"
