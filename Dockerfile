FROM rust:1.23.0
MAINTAINER J. Dumont <j.dumont@coinamics.io>

RUN apt-get update && apt-get install -y libssl-dev pkg-config 
EXPOSE 3014

ENV appname server-ticksaver
ENV datafolder /coinfolio/data


VOLUME ["/coinfolio/data"] 
RUN mkdir -p /coinfolio && mkdir /coinfolio/${appname} && mkdir -p /coinfolio/data
ADD target/release/server-ticksaver /coinfolio/${appname}
ADD Cargo.toml /coinfolio/${appname}/
ADD src /coinfolio/${appname}/
RUN cd /coinfolio/${appname}
RUN cargo install
RUN cargo run --features ssl --example cli wss://ws-feed.exchange.coinbase.com/

RUN chmod 777 /coinfolio/${appname}/server-ticksaver
RUN chmod 777 -R ${datafolder}
RUN ulimit -n 2048
CMD exec /coinfolio/${appname}/server-ticksaver ${datafolder}