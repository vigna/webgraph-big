FROM ubuntu:22.04

# install the basic deps
RUN apt-get update -qy && \
    apt install -qy default-jdk ivy ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# make a symbolic link so that ant recognizes ivy
RUN ln -s -T /usr/share/java/ivy.jar /usr/share/ant/lib/ivy.jar
# Setup env vars
ENV JAVA_HOME /lib/jvm/java-11-openjdk-amd64/
ENV ANT_HOME /usr/share/ant/

# To build:
# docker build . -f ./Dockerfile -t webgraph-big

# To compile webgraph:
# docker run -it -v "$PWD:/io" webgraph-big bash
# cd /io
# ant ivy-setupjars jar
