FROM ubuntu:16.04
MAINTAINER J. Dumont <j.dumont@coinamics.io>

EXPOSE 3014

ENV appname server-ticksaver

RUN mkdir /coinfolio && mkdir /coinfolio/${appname}
ADD target/release/server-ticksaver /coinfolio/${appname}
CMD exec /coinfolio/${appname}/server-ticksaver