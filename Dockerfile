# build with
#    docker build -t libvroom .
# run with
#    docker run -it --privileged -v $(pwd):/project:Z libvroom /bin/bash

FROM ubuntu:20.10

RUN apt-get update -qq
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata g++ git make gdb

# This seems needed to enable stl pretty printing in gdb
RUN echo "sys.path.insert(0, '/usr/share/gcc-10/python/')"  > ~/.gdbinit

RUN mkdir project

WORKDIR /project
