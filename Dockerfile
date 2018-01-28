FROM ubuntu:16.04
MAINTAINER J. Dumont <j.dumont@coinamics.io>

RUN apt-get update && apt-get install -y libssl-dev pkg-config 
EXPOSE 3014

ENV appname server-ticksaver
ENV datafolder /coinfolio/data

VOLUME ["/coinfolio/data"] 
RUN mkdir -p /coinfolio && mkdir /coinfolio/${appname} && mkdir -p /coinfolio/data
ADD target/release/server-ticksaver /coinfolio/${appname}
RUN chmod 777 /coinfolio/${appname}/server-ticksaver
RUN chmod 777 -R ${datafolder}
RUN ulimit -n 2048
CMD exec /coinfolio/${appname}/server-ticksaver ${datafolder}