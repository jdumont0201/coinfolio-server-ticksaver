extern crate ws;
extern crate serde;
extern crate serde_json;
extern crate json;
extern crate clap;
extern crate ansi_term;

use ansi_term::Colour::*;
//use clap::{Arg, App, SubCommand};
use std::sync::{Arc, Mutex};

use std::rc::Rc;
use std::cell::Cell;

#[macro_use]
extern crate serde_derive;
extern crate chrono;

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::fs::OpenOptions;
use std::io::BufWriter;
use ws::{connect, Handler, Sender, Handshake, Result, Message, CloseCode, Response};
use std::thread;
use chrono::prelude::*;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
//use chrono::{NaiveDate, NaiveTime};

mod broker {
    //use std::io::Write;
    //use std::fs::OpenOptions;
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

fn concat(path: &str, file: &str) -> String {
    let mut owned_str: String = "".to_owned();
    owned_str.push_str(path);
    owned_str.push_str(file);
    owned_str.push_str(".csv");
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
    buffer_level: u8,
    buffer_max: u8,

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
    fn get_path(&mut self,broker_name: &str,  interval: &str) -> String {
        let symbol=&self.name;
        let args: Vec<_> = ::std::env::args().collect();
        let base_dir_name = &args[1];
        println!("[{}] Data folder: {}/{}/{}/{}", symbol, base_dir_name, broker_name,symbol,interval);

        let mut dir_name = base_dir_name.to_owned();
        dir_name.push_str("/");
        dir_name.push_str(broker_name);
        dir_name.push_str("/");
        dir_name.push_str(&symbol);
        dir_name.push_str("/");
        dir_name.push_str(interval);
        dir_name.push_str("/");

        //create directory
        let exists = std::path::Path::new(&dir_name).exists();
        if exists {
            println!("  File {} exists",dir_name);
        } else {
            println!("  File {} not exising", dir_name);
            let path = std::path::Path::new(&dir_name);
            if let Ok(r) = std::fs::create_dir_all(path) {
                println!("  Dir  Created");
            } else {
                println!("  {}", Red.paint("  ERR create_tick_bufwriter cannot create folder"));
            }
        }


        dir_name
    }
    fn set_paths(&mut self) {
        self.path_tick = self.get_path("binance",  "tick");
        self.path_1m = self.get_path("binance",  "1m");
        self.path_5m = self.get_path("binance",  "5m");
        println!("Save path {}", &self.path_tick);
    }
    pub fn update_interval_bar(&mut self, ohlc: &GenericOHLC, interval: u32) {
        println!("update {}", interval);
        use chrono::Timelike;
        let ts = ohlc.ts;
        let dt = ::chrono::NaiveDateTime::from_timestamp(ts / 1000, 0);
        let min = dt.minute();
        let rem = min % interval;

        println!("[{}] Collecting 5 min bar in pos {}", self.name,self.last_bar_5_position);
        self.last_bars_5[self.last_bar_5_position] = GenericOHLC {
            ts: ohlc.ts,
            o: ohlc.o,
            l: ohlc.l,
            h: ohlc.h,
            c: ohlc.c,
            v: ohlc.v,
        };

        if rem == interval - 1 { //time to save
            println!("[{}] Time to save 5m bar : {} {} {} {}",self.name, ts, dt, min, rem);
            let ohlc = self.generate_5m_bar(5);
            if let Some(ohlc_) = ohlc {

                let filename = ::chrono::Utc::now().format("%Y%m%d").to_string();
                let path=concat(&self.path_5m,&filename);
                self.save_interval_bar(&ohlc_, &path);
            }else{
                println!("[{}] 5m bar has empty ",self.name);
            }

        }
        self.last_bar_5_position = (self.last_bar_5_position + 1) % 5;
    }
    fn save_tick(&mut self, tick: &GenericTick) {
        self.write_tick_in_buffer(tick);
        self.buffer_level = self.buffer_level + 1;
        if self.is_buffer_full() {
            match self.writer {
                Some(ref mut w) => {
                    //write in buffer
                    println!("[{}] writing {} ticks from buffer", self.name ,self.buffer_max);
                    w.flush().unwrap();
                    self.buffer_level = 0;
                }
                None => {
                    println!("created buffer not exisitgng");
                }
            }
        }
    }

    fn is_buffer_full(&self) -> bool {
        self.buffer_level >= self.buffer_max
    }

    fn create_tick_bufwriter(&mut self, filename: &str) -> Result<BufWriter<File>> {
        let dir_name = &self.path_tick;
        let path = concat(&dir_name, &filename);
        println!("[{}] Create buffer at {}", self.name, path);

        //create directory
        let exists = std::path::Path::new(&path).exists();
        if exists {
            println!("  File {} exists", path);
        } else {
            println!("  File {} not exising", path);
            if let Ok(r) = std::fs::create_dir_all(dir_name) {
                println!("  Dir  {} Created", path);
            } else {
                println!("  {}", Red.paint("  ERR create_tick_bufwriter cannot create folder"));
            }
        }

        let file = OpenOptions::new().write(true).create(true).append(true).open(&path);
        match file {
            Ok(filed) => {
                println!("  create file ok");
                let mut w = BufWriter::new(filed);
                println!("  create file ok buffwrtier");
                Ok(w)
            }
            Err(err) => {
                println!("{}", Red.paint("  ERR create_tick_bufwriter file"));
                panic!("  called `Result::unwrap()` on an `Err` value: {:?}", err);
            }
        }
    }

    fn save_1m(&mut self, mut ohlc: &GenericOHLC) {
        if ohlc.ts != self.current_ts {
            println!("[{}] new tick {} {} {}",self.name, ohlc.ts, self.current_ts, ohlc.ts - self.current_ts);
            let diff = ohlc.ts - self.current_ts;
            if (diff == 60000 || self.current_ts == 0) {} else {
                println!("  !!! Missing tick !!! ")
            }
            self.old_ts = self.current_ts;
            self.current_ts = ohlc.ts;
        } else {
            println!("same tick")
        }
        self.update_interval_bar(ohlc, 5);


        let filename = ::chrono::Utc::now().format("%Y%m%d").to_string();
        let path = ::concat(&self.path_1m, &filename);
        println!("[{}]  ->  save 1m in {}", self.name,&path);
        let file = OpenOptions::new().write(true).create(true).append(true).open(path);
        let content_1m = ohlc.to_string();
        if let Ok(mut filed) = file {
            println!("[{}] File ok",self.name);
            let mut s = content_1m.as_bytes();
            filed.write_all(s).unwrap();
        } else {
            println!("[{}] {} ",self.name,Red.paint("Cannot save 1m"));
        }
    }

    fn write_tick_in_buffer(&mut self, tick: &GenericTick) {
        match self.writer {
            Some(ref mut w) => {
                let mut owned_str = tick.to_string();
                w.write_all(owned_str.as_bytes()).unwrap();
            }
            None => {}
        }
    }

    pub fn save_interval_bar(&mut self, mut ohlc: &GenericOHLC, path: &String) {
        println!("[{}]  ->  save 5m in {}", self.name,path);
        let file = OpenOptions::new().write(true).create(true).append(true).open(path);
        let content = ohlc.to_string();

        if let Ok(mut filed) = file {
            let mut s = content.as_bytes();
            filed.write_all(s).unwrap();
        } else {}
    }

    fn generate_5m_bar(&mut self, size: usize) -> Option<GenericOHLC> {
        let last = self.last_bar_5_position;
        let first=(last+1) % size;
        let ts: i64 = self.last_bars_5.get(first).unwrap().ts;
        let o: f64 = self.last_bars_5.get(first).unwrap().o;
        let mut h: f64 = -1000000000.;
        let mut l: f64 = 10000000000.;
        let c: f64 = self.last_bars_5.get(last).unwrap().c;
        let mut v: f64 = 0.;

        let mut hasEmpty=false;
        for i in 0..size {
            let bar = self.last_bars_5.get(i).unwrap();
            if h < bar.h {
                h = bar.h;
            }
            if l > bar.l {
                l = bar.l;
            }
            if bar.c < 0.0000000001 {
                hasEmpty=true;
            }
            v = v + self.last_bars_5.get(i).unwrap().v;
        }
        if hasEmpty{
            None
        }else{
            Some(GenericOHLC { ts: ts, o: o, h: h, l: l, c: c, v: v })
        }

    }

    fn check_change_bufwriter_name(&mut self) {
        let todaystr = chrono::Utc::now().format("%Y%m%d").to_string();
        if todaystr == self.last_today_str {
            //println!("same file");
        } else {
            match self.writer {
                Some(ref mut w) => {
                    println!("[{}] --> Changebuffer  write last buffer", self.name);
                    w.flush().unwrap();
                }
                None => {}
            }
            self.buffer_level = 0;
            let filename = chrono::Utc::now().format("%Y%m%d %M %S").to_string();
            let path = concat(&self.path_tick, &todaystr);
            println!("[{}] --> Changebuffer Changing buffer file to {}", self.name, filename);
            self.reset_tick_buffer();
            self.last_today_str = todaystr;
        }
    }

    fn reset_tick_buffer(&mut self) {
        let filename = chrono::Utc::now().format("%Y%m%d").to_string();
        let w = self.create_tick_bufwriter(&filename);
        if let Ok(wr) = w {
            self.writer = Some(wr);
        } else {
            println!("Error opening bufwriter {}", &self.path_tick)
        }
    }
}

impl Handler for Client {
    fn on_open(&mut self, _: Handshake) -> Result<()> {
        println!("Open ws");
        self.set_paths();
        for i in 0..5 {
            self.last_bars_5.push(GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. });
        }
        self.reset_tick_buffer();
        self.out.send("Hello WebSocket")
    }
    fn on_error(&mut self, err: ws::Error) {
        println!("err {}", err);
    }
    fn on_response(&mut self, _res: &Response) -> Result<()> {
        self.out.send("Hello WebSocket res")
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        let m = msg.as_text().unwrap();
        let mut v: broker::ParsedBrokerMessage = serde_json::from_str(m).unwrap();
//parse for later addition
        let vol = v.k.v.parse::<f64>().unwrap();
        //self.volume_1m = self.volume_1m + vol;


        let tick = v.get_tick();
        self.check_change_bufwriter_name();
        self.save_tick(&tick);
        if v.k.x {//isfinal
            let mut ohlc = v.get_generic_OHLC();
            self.save_1m(&ohlc);
        }
        self.out.close(CloseCode::Normal)
    }
}

fn main() {
    let mut children = vec![];
    //let pairs = vec!["ETHUSDT","BTCUSDT","BNBUSDT","NEOUSDT","LTCUSDT","BBCUSDT"];
    static PAIRS: &'static [&str] = &["ETHBTC" /*, "LTCBTC", "BNBBTC", "NEOBTC", "123456", "QTUMETH", "EOSETH", "SNTETH", "BNTETH", "BCCBTC",
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

  ,                   "WINGSBTC", "WINGSETH", "NAVBTC", "NAVETH", "NAVBNB", "LUNBTC", "LUNETH", "TRIGBTC",
                     "TRIGETH", "TRIGBNB", "APPCBTC", "APPCETH", "APPCBNB", "VIBEBTC", "VIBEETH", "RLCBTC",
                     "RLCETH", "RLCBNB", "INSBTC", "INSETH", "PIVXBTC", "PIVXETH", "PIVXBNB", "IOSTBTC", "IOSTETH"*/
    ];
//let pairs = vec!["ETHUSDT"];

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

    for p in PAIRS.iter() {
        let pp = p.clone();
        let data_inner = data.clone();
        children.push(thread::spawn(move || {
            let url = broker::get_url(pp.to_string());
            println!("Launching thread {} {}", pp.to_string(), url);
            connect(url, |out| Client {
                out: out,
                name: pp.to_string(),
                path_tick: "".to_string(),

                buffer_level: 0,
                buffer_max: 20,
                current_ts: 0,
                old_ts: 0,
                path_1m: "".to_string(),
                path_5m: "".to_string(),

//count: count.clone(),
                writer: None,
                last_today_str: " ".to_string(),
                last_bar_5_position: 0,
                last_bars_5: Vec::new(),
                bar_5m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
                bar_15m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
                bar_30m: GenericOHLC { ts: 0, o: 0., h: 0., l: 0., c: 0., v: 0. },
//data: client_data.clone(),
//nbUpdated:*daa
            }).unwrap();
        }));
    }
    for child in children {
        let _ = child.join();
    }
}