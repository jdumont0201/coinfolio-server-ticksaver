extern crate ws;
extern crate serde;
extern crate serde_json;
extern crate json;
extern crate clap;
extern crate ansi_term;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate reqwest;

#[macro_use]
extern crate serde_derive;
extern crate chrono;


use ansi_term::Colour::*;
use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::Cell;
use std::io::{self};
use futures::{Future, Stream};
use tokio_core::reactor::Core;
use hyper::header::{ContentLength, ContentType};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::fs::OpenOptions;
use std::io::BufWriter;
use ws::{connect, Handler, Sender, Handshake, Result, Message, CloseCode, Response};
use std::thread;
use chrono::prelude::*;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};

static WRITE_ON_DISK: bool = false;
static SEND_TO_DB: bool = true;

mod broker {
    use super::Client;
    use std::env;
    use std::str;
    use std::str::FromStr;


    pub fn get_url(pair: String) -> String {
        let mut s = "wss://stream.binance.com:9443/ws/".to_owned();
        let pairl = pair.to_lowercase();
        s.push_str(&pairl);
        s.push_str("@kline_1m");
        s
    }

    #[derive(Serialize, Deserialize)]
    pub struct RawTick {
        B: String,
        L: i64,
        Q: String,
        T: i64,
        V: String,
        pub c: String,
        f: i64,
        pub h: String,
        i: String,
        pub l: String,
        n: i64,
        pub o: String,
        q: String,
        s: String,
        pub t: i64,
        pub v: String,
        pub x: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub struct ParsedBrokerMessage {
        E: i64,
        e: String,
        pub k: RawTick,
        pub  s: String,
    }

    impl ParsedBrokerMessage {
        pub fn get_tick(&self) -> ::GenericTick {
            ::GenericTick {
                ts: self.k.t,
                p: super::parsef64(&self.k.c),
                v: super::parsef64(&self.k.v),
            }
        }
        pub fn get_generic_OHLC(&self) -> ::GenericOHLC {
            ::GenericOHLC {
                ts: self.k.t,
                o: super::parsef64(&self.k.o),
                h: super::parsef64(&self.k.h),
                l: super::parsef64(&self.k.l),
                c: super::parsef64(&self.k.c),
                v: super::parsef64(&self.k.v),
            }
        }
    }
}

fn parsei64(i: &String) -> i64 {
    i.parse::<i64>().unwrap()
}

fn parsef64(i: &String) -> f64 {
    i.parse::<f64>().unwrap()
}

fn concat(a: &str, b: &str) -> String {
    let mut owned_str: String = "".to_owned();
    owned_str.push_str(a);
    owned_str.push_str(b);
    owned_str
}

pub struct GenericTick {
    ts: i64,
    p: f64,
    v: f64,
}

impl GenericTick {
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.p.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
    fn to_json(&self, broker: &str, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","close" :"{}","volume":"{}"}}"#, ts, pair, self.p, self.v);
        s
    }
}

pub struct GenericOHLC {
    ts: i64,
    o: f64,
    h: f64,
    c: f64,
    l: f64,
    v: f64,
}

impl GenericOHLC {
    fn to_json(&self, broker: &str, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","open"  :"{}","high"  :"{}","low":"{}","close":"{}","volume":"{}"}}"#, ts, pair, self.o, self.h, self.l, self.c, self.v);
        s
    }
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.o.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.h.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.l.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.c.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
}

pub struct Client {
    out: Sender,
    writer: Option<BufWriter<File>>,
    name: String,
    broker: String,

    buffer_level: u8,
    buffer_max: u8,
    textbuffer: String,

    ohlc_1m_buffer_level:u8,
    ohlc_1m_buffer_max: u8,
    ohlc_1m_textbuffer: String,

    client: reqwest::Client,
    last_today_str: String,
    path_tick: String,

    current_ts: i64,
    old_ts: i64,

    path_1m: String,
    path_5m: String,

    last_bars_5: std::vec::Vec<GenericOHLC>,
    last_bar_5_position: usize,

    bar_5m: GenericOHLC,
    bar_15m: GenericOHLC,
    bar_30m: GenericOHLC,
}

impl Client {
    fn save_tick(&mut self, tick: &GenericTick) {
        self.write_tick_in_buffer(tick);

        let mut doSend = false;
        self.buffer_level = self.buffer_level + 1;
        if self.is_buffer_full() {
            doSend = true;
            self.buffer_level = 0;
        } else {
            self.textbuffer = concat(&self.textbuffer, ",");
        }
        if doSend {
            let json = concat(&self.textbuffer, &"]".to_string());
            let uri = format!("http://0.0.0.0:3000/{}_tick", self.broker);
            //println!("[POST] ticks {}", json);
            if let Ok(mut res) = self.client.post(&uri).body(json).send() {
                println!("[{}] [POST] {}_tick {} {}",self.name, self.broker, res.status(), res.text().unwrap());
                self.textbuffer = "[".to_string();
            } else {
                println!("[{}] [POST] nok uri",self.name);
            }
        }
    }

    fn is_buffer_full(&self) -> bool {
        self.buffer_level >= self.buffer_max
    }

    fn save_1m(&mut self, mut ohlc: &GenericOHLC) {
        if ohlc.ts != self.current_ts {
            println!("[{}] ohlc_1m {} {} {}", self.name, ohlc.ts, self.current_ts, ohlc.ts - self.current_ts);
            let diff = ohlc.ts - self.current_ts;
            if (diff == 60000 || self.current_ts == 0) {} else {
                println!("  !!! Missing tick !!! ")
            }
            self.old_ts = self.current_ts;
            self.current_ts = ohlc.ts;
        } else {
            println!("same tick")
        }

        //self.ohlc_1m_textbuffer = concat(&self.ohlc_1m_textbuffer, &ohlc.to_json(&self.broker, &self.name));


        //send request post
        let json = ohlc.to_json("BIN", &self.name);
        let uri = format!("http://0.0.0.0:3000/{}_ohlc_1m", self.broker);
        if let Ok(mut res) = self.client.post(&uri).body(json).send() {
            println!("[{}] [POST] {}_ohlc_1m {} {}",self.name, self.broker, res.status(), res.text().unwrap());
        } else {
            println!("[{}] [POST] nok uri",self.name);
        }
    }

    fn write_tick_in_buffer(&mut self, tick: &GenericTick) {
        self.textbuffer = concat(&self.textbuffer, &tick.to_json(&self.broker, &self.name));
    }
}

impl Handler for Client {
    fn on_open(&mut self, _: Handshake) -> Result<()> {
        println!("[{}] Open ws", self.name);

        self.out.send("Hello WebSocket")
    }
    fn on_error(&mut self, err: ws::Error) {
        println!("err kind{:?}", err.kind);
        println!("err {}", err);
    }
    fn on_response(&mut self, _res: &Response) -> Result<()> {
        self.out.send("Hello WebSocket res")
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        let m = msg.as_text().unwrap();
        let mut v: broker::ParsedBrokerMessage = serde_json::from_str(m).unwrap();
        let tick = v.get_tick();
        self.save_tick(&tick);
        if v.k.x {//isfinal
            let mut ohlc = v.get_generic_OHLC();
            self.save_1m(&ohlc);
        }
        self.out.close(CloseCode::Normal)
    }
}

fn main() {
    println!("Coinamics Server Websockets");
    let mut children = vec![];
    //let pairs = vec!["ETHUSDT","BTCUSDT","BNBUSDT","NEOUSDT","LTCUSDT","BBCUSDT"];
    static PAIRS: &'static [&str] = &["ETHBTC", "LTCBTC", "BNBBTC", "NEOBTC", "123456", "QTUMETH", "EOSETH", "SNTETH", "BNTETH", "BCCBTC",
        "GASBTC", "BNBETH", "BTCUSDT", "ETHUSDT", "HSRBTC", "OAXETH", "DNTETH", "MCOETH", "ICNETH", "MCOBTC",
        "WTCBTC", "WTCETH", "LRCBTC", "LRCETH", "QTUMBTC", "YOYOBTC", "OMGBTC", "OMGETH", "ZRXBTC", "ZRXETH",
        "STRATBTC", "STRATETH", "SNGLSBTC", "SNGLSETH", "BQXBTC", "BQXETH", "KNCBTC", "KNCETH", "FUNBTC",
        "FUNETH", "SNMBTC", "SNMETH", "NEOETH", "IOTABTC", "IOTAETH", "LINKBTC", "LINKETH", "XVGBTC"

        , "XVGETH", "CTRBTC", "CTRETH", "SALTBTC", "SALTETH", "MDABTC", "MDAETH", "MTLBTC", "MTLETH",
        "SUBBTC", "SUBETH", "EOSBTC", "SNTBTC", "ETCETH", "ETCBTC", "MTHBTC", "MTHETH", "ENGBTC",
        "ENGETH", "DNTBTC", "ZECBTC", "ZECETH", "BNTBTC", "ASTBTC", "ASTETH", "DASHBTC", "DASHETH",
        "OAXBTC", "ICNBTC", "BTGBTC", "BTGETH", "EVXBTC", "EVXETH", "REQBTC", "REQETH", "VIBBTC",
        "VIBETH", "HSRETH", "TRXBTC", "TRXETH", "POWRBTC", "POWRETH", "ARKBTC", "ARKETH", "YOYOETH",
        "XRPBTC", "XRPETH", "MODBTC", "MODETH", "ENJBTC", "ENJETH", "STORJBTC", "STORJETH", "BNBUSDT",
        "VENBNB", "YOYOBNB", "POWRBNB", "VENBTC", "VENETH", "KMDBTC", "KMDETH", "NULSBNB", "RCNBTC",
        "RCNETH", "RCNBNB", "NULSBTC", "NULSETH", "RDNBTC", "RDNETH", "RDNBNB", "XMRBTC", "XMRETH",
        "DLTBNB", "WTCBNB", "DLTBTC", "DLTETH", "AMBBTC", "AMBETH", "AMBBNB", "BCCETH", "BCCUSDT",
        "BCCBNB", "BATBTC", "BATETH", "BATBNB", "BCPTBTC", "BCPTETH", "BCPTBNB", "ARNBTC", "ARNETH",
        "GVTBTC", "GVTETH", "CDTBTC", "CDTETH", "GXSBTC", "GXSETH", "NEOUSDT", "NEOBNB", "POEBTC",
        "POEETH", "QSPBTC", "QSPETH", "QSPBNB", "BTSBTC", "BTSETH", "BTSBNB", "XZCBTC", "XZCETH",
        "XZCBNB", "LSKBTC", "LSKETH", "LSKBNB", "TNTBTC", "TNTETH", "FUELBTC", "FUELETH", "MANABTC",
        "MANAETH", "BCDBTC", "BCDETH", "DGDBTC", "DGDETH", "IOTABNB", "ADXBTC", "ADXETH", "ADXBNB",
        "ADABTC", "ADAETH", "PPTBTC", "PPTETH", "CMTBTC", "CMTETH", "CMTBNB", "XLMBTC", "XLMETH",
        "XLMBNB", "CNDBTC", "CNDETH", "CNDBNB", "LENDBTC", "LENDETH", "WABIBTC", "WABIETH",
        "WABIBNB", "LTCETH", "LTCUSDT", "LTCBNB", "TNBBTC", "TNBETH", "WAVESBTC", "WAVESETH",
        "WAVESBNB", "GTOBTC", "GTOETH", "GTOBNB", "ICXBTC", "ICXETH", "ICXBNB", "OSTBTC",
        "OSTETH", "OSTBNB", "ELFBTC", "ELFETH", "AIONBTC", "AIONETH", "AIONBNB", "NEBLBTC",
        "NEBLETH", "NEBLBNB", "BRDBTC", "BRDETH", "BRDBNB", "MCOBNB", "EDOBTC", "EDOETH",
        "WINGSBTC", "WINGSETH", "NAVBTC", "NAVETH", "NAVBNB", "LUNBTC", "LUNETH", "TRIGBTC",
        "TRIGETH", "TRIGBNB", "APPCBTC", "APPCETH", "APPCBNB", "VIBEBTC", "VIBEETH", "RLCBTC",
        "RLCETH", "RLCBNB", "INSBTC", "INSETH", "PIVXBTC", "PIVXETH", "PIVXBNB", "IOSTBTC", "IOSTETH"
    ];

    let nb = PAIRS.len();

    let mut c: HashMap<String, usize> = HashMap::new();//Vec::with_capacity(nb);//(0..nb).collect();
    for i in 0..nb {
        c.insert(PAIRS[i].to_string(), 0);
    }


    let mut v: Vec<usize> = Vec::with_capacity(nb);//(0..nb).collect();
    for i in 0..nb {
        v.push(0);
    }
    println!("{:?}", c);
    let data = Arc::new(Mutex::new(c));
    let count = Rc::new(Cell::new(0));
    let client_data = v.clone();

    println!("Starting pair threads");
    for p in PAIRS.iter() {
        println!("Starting pair thread {}", p);
        let pp = p.clone();
        let bb = "bin";
        let data_inner = data.clone();
        children.push(thread::spawn(move || {
            let url = broker::get_url(pp.to_string());
            let client = reqwest::Client::new();
            println!("Launching thread {} {}", pp.to_string(), url);
            connect(url, |out| Client {
                out: out,
                name: pp.to_string(),
                broker: bb.to_string(),
                path_tick: "".to_string(),

                buffer_level: 0,
                buffer_max: 20,
                textbuffer: "[".to_string(),

                ohlc_1m_buffer_level: 0,
                ohlc_1m_buffer_max: 20,
                ohlc_1m_textbuffer:"[".to_string(),

                current_ts: 0,
                old_ts: 0,

                client: client.clone(),
                path_1m: "".to_string(),
                path_5m: "".to_string(),

                writer: None,

                last_today_str: " ".to_string(),
                last_bar_5_position: 0,
                last_bars_5: Vec::new(),

                bar_5m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
                bar_15m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
                bar_30m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
            }).unwrap();
        }));
    }
    for child in children {
        let _ = child.join();
    }
}