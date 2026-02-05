FROM gcr.io/oss-fuzz-base/base-builder
RUN apt-get update && apt-get install -y cmake
COPY . $SRC/libvroom
WORKDIR $SRC/libvroom
