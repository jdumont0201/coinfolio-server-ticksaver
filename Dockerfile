FROM ubuntu:16.04
MAINTAINER J. Dumont <j.dumont@coinamics.io>
VOLUME ["/coinfolio/data"]
EXPOSE 3014
RUN apt-get update && apt-get install -y libssl-dev pkg-config ca-certificates


ENV appname server-ticksaver
ENV datafolder /coinfolio/data




RUN mkdir -p /coinfolio && mkdir /coinfolio/${appname} && mkdir -p /coinfolio/data
#ADD Cargo.toml /coinfolio/${appname}/
#ADD . /coinfolio/${appname}
ADD target/release/server-ticksaver /coinfolio/${appname}
#RUN cd /coinfolio/${appname} && cargo install


RUN chmod 777 /coinfolio/${appname}/server-ticksaver && chmod 777 -R ${datafolder} &&  ulimit -n 2048
CMD exec /coinfolio/${appname}/server-ticksaver ${pairs}