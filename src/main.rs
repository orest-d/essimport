extern crate clap;
extern crate zip;
extern crate xml;
extern crate rustc_serialize;
extern crate bson;
extern crate mongodb;

use std::collections::BTreeMap;
use clap::{Arg, App};
use std::io::{BufReader, Read, Write};
use std::fs::File;
use std::str;
use xml::reader::{EventReader, XmlEvent};
use rustc_serialize::json::{ToJson, Json, Object, Array};
use bson::{Bson, Document};
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;


fn is_struct(name: &str) -> bool {
    match name {
        "Statistik" => true,
        "KoeretoejAnvendelseStruktur" => true,
        "KoeretoejOplysningGrundStruktur" => true,
        "KoeretoejBetegnelseStruktur" => true,
        "Model" => true,
        "Variant" => true,
        "Type" => true,
        "KoeretoejFarveStruktur" => true,
        "FarveTypeStruktur" => true,
        "KarrosseriTypeStruktur" => true,
        "KoeretoejNormStruktur" => true,
        "NormTypeStruktur" => true,
        "KoeretoejMiljoeOplysningStruktur" => true,
        "KoeretoejMotorStruktur" => true,
        "DrivkraftTypeStruktur" => true,
        "EjerBrugerSamling" => true,
        "EjerBruger" => true,
        "EjerBrugerForholdGrundStruktur" => true,
        "TilladelseSamling" => true,
        "Tilladelse" => true,
        "TilladelseStruktur" => true,
        "TilladelseTypeStruktur" => true,
        "KoeretoejSupplerendeKarrosseriSamlingStruktur" => true,
        "KoeretoejSupplerendeKarrosseriSamling" => true,
        "KoeretoejSupplerendeKarrosseriTypeStruktur" => true,
        "SynResultatStruktur" => true,
        "KoeretoejBlokeringAarsagListeStruktur" => true,
        "KoeretoejBlokeringAarsagListe" => true,
        "KoeretoejBlokeringAarsag" => true,
        "KoeretoejUdstyrSamlingStruktur" => true,
        "KoeretoejUdstyrSamling" => true,
        "KoeretoejUdstyrStruktur" => true,
        "KoeretoejUdstyrTypeStruktur" => true,
        "DispensationTypeSamlingStruktur" => true,
        "DispensationTypeSamling" => true,
        "DispensationTypeStruktur" => true,
        "TilladelseTypeDetaljeValg" => true,
        "KunGodkendtForJuridiskEnhed" => true,
        "JuridiskEnhedIdentifikatorStruktur" => true,
        "JuridiskEnhedValg" => true,
        "KoeretoejAnvendelseSamlingStruktur" => true,
        "KoeretoejAnvendelseSamling" => true,
        "KoeretoejFastKombination" => true,
        "FastTilkobling" => true,
        "VariabelKombination" => true,
        "KoeretoejGenerelIdentifikatorStruktur" => true,
        "KoeretoejGenerelIdentifikatorValg" => true,
        "PENummerCVR" => true,
        _ => false
    }
}

fn is_array(name: &str) -> bool {
    match name {
        "DispensationTypeSamling" => true,
        "EjerBrugerSamling" => true,
        "KoeretoejAnvendelseSamling" => true,
        "KoeretoejBlokeringAarsagListe" => true,
        "KoeretoejSupplerendeKarrosseriSamling" => true,
        "KoeretoejUdstyrSamling" => true,
        "TilladelseSamling" => true,
        _ => false
    }
}
//TODO: KoeretoejOplysningGrundStruktur is a special case containing 0,1 or 2 KoeretoejFastKombination

/*
fn do_read(r:&mut Read){
    let mut buff = [0u8;20];
    r.read(&mut buff);
    println!("{}",str::from_utf8(&buff).unwrap());
}
*/
#[derive(Debug)]
struct Record {
    element: String,
    is_struct: bool,
    text: String,
    structure: Vec<Record>
}

impl Record {
    fn new(element: &str) -> Record {
        Record {
            element: String::from(element),
            is_struct: is_struct(element),
            text: String::new(),
            structure: Vec::new()
        }
    }
    fn add_text(&mut self, text: &str) {
        self.text.push_str(text);
    }
    fn add_child(&mut self, rec: Record) {
        self.structure.push(rec);
    }
    fn to_bson(&self) -> Bson {
        if self.is_struct {
            if is_array(&self.element) {
                let mut array: bson::Array = Vec::new();
                for r in &self.structure {
                    array.push(r.to_bson());
                }
                Bson::Array(array)
            } else {
                let mut obj: Document = Document::new();
                for r in &self.structure {
                    if obj.contains_key(&r.element) {
                        println!("ERROR: Multiple {} inside {}", r.element, self.element);
                    } else {
                        obj.insert_bson(r.element.clone(), r.to_bson());
                    }
                }
                Bson::Document(obj)
            }
        } else {
            Bson::String(self.text.clone())
        }
    }
}

impl ToJson for Record {
    fn to_json(&self) -> Json {
        if self.is_struct {
            if is_array(&self.element) {
                let mut array: Array = Vec::new();
                for r in &self.structure {
                    array.push(r.to_json());
                }
                Json::Array(array)
            } else {
                let mut obj: Object = BTreeMap::new();
                for r in &self.structure {
                    if obj.contains_key(&r.element) {
                        println!("ERROR: Multiple {} inside {}", r.element, self.element);
                    } else {
                        obj.insert(r.element.clone(), r.to_json());
                    }
                }
                Json::Object(obj)
            }
        } else {
            Json::String(self.text.clone())
        }
    }
}

fn do_read(r: &mut Read, json_output: Option<&str>, json_chunk: Option<&str>, mongodb_uri:Option<&str>, db:&str, collection:&str) {
    let mut number = 0;
    let file = BufReader::new(r);
    let parser = EventReader::new(file);
    let mut stack: Vec<Record> = Vec::new();
    let client = mongodb_uri.map(|uri|Client::with_uri(uri).expect("Failed to initialize client."));
    let coll = client.map(|c|c.db(db).collection(collection));
    let jsonchunk:usize = json_chunk.map(|x|x.parse::<usize>().expect("Unsigned number expected as jsonchunk parameter")).unwrap_or(1);
    if coll.is_some(){
        println!("Export to {}, database: {}, collection: {}",mongodb_uri.unwrap(),db,collection)
    }

    let mut output_file = None;

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
//                println!("Start {}", name.local_name);
                if !stack.is_empty() || name.local_name == "Statistik" {
                    stack.push(Record::new(&name.local_name));
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
//                println!("End   {}", name);
                if let Some(rec) = stack.pop() {
                    if stack.is_empty() {
                        if (number%jsonchunk==0) && (json_output.is_some()){
                            let path = &format!("{}/{}.json", json_output.unwrap(), number);
                            output_file = Some(File::create(path).expect(&format!("Can't create output file {}",path)));
                        }
                        number += 1;
                        if number%100 == 0{
                            println!("{}",number);
                        }
//                        println!("--> {:?}", rec);
//                        println!("JSON: {}", rec.to_json());
                        if let Some(ref mut f) = output_file{
                            f.write_all(format!("{}", rec.to_json()).as_bytes()).expect("Error writing document");
                            f.write("\n".as_bytes()).expect("Error writing newline");
                        }
                        if let Some(ref c) = coll{
                            if let bson::Bson::Document(document) = rec.to_bson(){
//mc                                println!("insert {}",document);
                                c.insert_one(document,None).expect("Insert error");
                            }else{
                                println!("Not a document");
                            }
                        }
                    } else {
                        if let Some(mut parent) = stack.pop() {
                            parent.add_child(rec);
                            stack.push(parent);
                        }
                    }
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                //println!("Text  {}", text);

                if let Some(mut rec) = stack.pop() {
                    rec.add_text(&text);
                    stack.push(rec);
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

}

fn main() {
    let matches = App::new("ESStatistik import")
        .version("1.0")
        .author("Orest Dubay <orest3.dubay@gmail.com>")
        .about("Import data from ESStatistikListeModtag")
        .arg(Arg::with_name("input")
            .short("i")
            .long("input")
            .value_name("FILE")
            .help("Inport from FILE")
            .takes_value(true))
        .arg(Arg::with_name("format")
            .short("f")
            .long("format")
            .value_name("FMT")
            .help("Input format")
            .takes_value(true))
        .arg(Arg::with_name("json")
            .short("j")
            .long("json")
            .value_name("PATH")
            .help("Export to json files")
            .takes_value(true))
        .arg(Arg::with_name("jsonchunk")
            .short("n")
            .long("jsonchunk")
            .value_name("SIZE")
            .help("Number of records in an import chunk")
            .takes_value(true))
        .arg(Arg::with_name("mongodb")
            .short("m")
            .long("mongodb")
            .value_name("URI")
            .help("Export to MongoDB, specify URI, e.g. mongodb://localhost:27017")
            .takes_value(true))
        .arg(Arg::with_name("db")
            .short("d")
            .long("db")
            .value_name("DATABASE")
            .help("MongoDB database, db test default")
            .takes_value(true))
        .arg(Arg::with_name("collection")
            .short("c")
            .long("collection")
            .value_name("COLLECTION")
            .help("MongoDB collection, ess by default")
            .takes_value(true))
        .get_matches();

    let input = matches.value_of("input").unwrap_or("ESStatistikListeModtag.xml");
    let format = matches.value_of("format").unwrap_or("xml");
    println!("file:   {}", input);
    println!("format: {}", format);
    match format {
        "xml" => {
            println!("READ XML {}", input);
            do_read(
                &mut File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input)),
                matches.value_of("json"),
                matches.value_of("jsonchunk"),
                matches.value_of("mongodb"),
                matches.value_of("db").unwrap_or("test"),
                matches.value_of("collection").unwrap_or("ess")
            )
        },

        "zip" => {
            println!("READ ZIP {}", input);
            let f = File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input));
            let mut archive = Box::new(zip::ZipArchive::new(f).unwrap_or_else(|err| panic!("{}\nCan't open zip archive {}", err, input)));
            do_read(
                &mut archive.by_index(0).unwrap_or_else(|err| panic!("{}\nCan't open zipped file {}", err, input)),
                matches.value_of("json"),
                matches.value_of("jsonchunk"),
                matches.value_of("mongodb"),
                matches.value_of("db").unwrap_or("test"),
                matches.value_of("collection").unwrap_or("ess")
            )
        }
        _ => panic!("Unsupported format: {}", format)
    };
}
