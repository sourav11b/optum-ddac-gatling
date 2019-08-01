#!/bin/sh

BUILD_TO=build/gatling-dse-sims

cat src/make/gatling-dse-launcher.sh build/libs/gatling-ddac-sims-optum-1.0.jar > ${BUILD_TO} && chmod 755 ${BUILD_TO}