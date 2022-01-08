extern crate rusqlite;
extern crate clap;
extern crate zip;
extern crate xml;
extern crate rustc_serialize;

use std::collections::BTreeMap;
use clap::Parser;
use std::io::{BufReader, Read, Write};
use std::fs::File;
use std::str;
use xml::reader::{EventReader, XmlEvent};
use rustc_serialize::json::{ToJson, Json, Object, Array};
use rusqlite::Connection;

enum ColType{
  String,
  Json,
  Int
}


static COLUMNS:[(&str, ColType);18] = [
("KoeretoejIdent",ColType::String),
("KoeretoejArtNummer",ColType::Int),
("KoeretoejArtNavn",ColType::String),
("KoeretoejAnvendelseStruktur",ColType::Json),
("RegistreringNummerNummer",ColType::String),
("RegistreringNummerUdloebDato",ColType::String),
("KoeretoejOplysningGrundStruktur",ColType::String),
("EjerBrugerSamling",ColType::Json),
("KoeretoejRegistreringStatus",ColType::String),
("KoeretoejRegistreringStatusDato",ColType::String),
("TilladelseSamling",ColType::Json),
("SynResultatStruktur",ColType::Json),
("AdressePostNummer",ColType::String),
("LeasingGyldigFra",ColType::String),
("LeasingGyldigTil",ColType::String),
("RegistreringNummerRettighedGyldigFra",ColType::String),
("RegistreringNummerRettighedGyldigTil",ColType::String),
("KoeretoejAnvendelseSamlingStruktur",ColType::Json)
];

fn create_table_sqlite() -> String{
    let mut sql = String::new();
    sql += "CREATE TABLE IF NOT EXISTS stat(\n";


    for (i,x) in COLUMNS.iter().enumerate(){
        let (name, ref ctype)=*x;

        sql+=&match *ctype{
            ColType::String => format!("  {:36} TEXT",name),
            ColType::Json => format!("  {:36} TEXT",name),
            ColType::Int => format!("  {:36} INTEGER",name)
        };
        if i< COLUMNS.len()-1{
            sql+=",\n";
        }
    }
    sql+=");\n";
    sql
}

fn insert_sqlite(r:&Object) -> String{

    let mut sql = String::new();
    sql += "INSERT INTO stat (\n";
    sql += &COLUMNS.iter().map(|x|x.0).collect::<Vec<&str>>().join(", ");
    sql += ")\nVALUES (";

    for (i,x) in COLUMNS.iter().enumerate(){
        let (name, ref ctype)=*x;
        if let Some(value) = r.get(name){
            sql+=&match *ctype{
                ColType::String => {
                    match *value{
                        Json::String(ref s) => format!("  '{}'",s),
                        _ => format!("  '{}'",value.to_json())
                    }
                },
                ColType::Json => format!("  '{}'",value.to_json()),
                ColType::Int => {
                    match *value {
                        Json::I64(x) => format!("  {}", x),
                        Json::U64(x) => format!("  {}", x),
                        _ => "  0".to_string()
                    }
                }
            };
        }
        else{
            sql+="  NULL";
        }
        if i< COLUMNS.len()-1{
            sql+=",\n";
        }
    }
    sql+=");\n";
    sql
}

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

fn do_read(r: &mut dyn Read, json_output: Option<String>, sqlite_output: Option<String>, chunksize: usize) {
    let mut number = 0;
    let file = BufReader::new(r);
    let parser = EventReader::new(file);
    let mut stack: Vec<Record> = Vec::new();

    let mut json_output_file = None;
    let sqlite = sqlite_output.map(|path| Connection::open(&path).expect(&format!("Can't open sqlite file {}",&path)));
    if let Some(ref connection) = sqlite{
        connection.execute(&create_table_sqlite(),[]).expect("Can't create sqlite table");
    }

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
                        if let Some(path) = &json_output.as_ref().map(|x| format!("{}/{}.json", x, number)){
                            if number%chunksize==0{
                                json_output_file = Some(File::create(path).expect(&format!("Can't create json output file {}",path)));
                            }
                        }
                        number += 1;
                        if number%1000 == 0{
                            println!("{}",number);
                        }
//                        println!("--> {:?}", rec);
//                        println!("JSON: {}", rec.to_json());
                        if let Json::Object(obj) = rec.to_json(){
                            println!("*** {}",insert_sqlite(&obj));
                            if let Some(ref connection) = sqlite{
                                connection.execute(&insert_sqlite(&obj),[]).expect("Can't create insert into sqlite table");
                                //connection.execute("COMMIT",&[]).expect("Can't commit sqlite insert");
                            }
                        }
                        if let Some(ref mut f) = json_output_file{
                            f.write_all(format!("{}", rec.to_json()).as_bytes()).expect("Error writing json document");
                            f.write("\n".as_bytes()).expect("Error writing newline");
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
                //break;
            }
            _ => {}
        }
    }

}

/// Import data from ESStatistikListeModtag
#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    /// File to import from
    #[clap(short, long, default_value_t = String::from("ESStatistikListeModtag.xml"))]
    input: String,

    /// Input format
    #[clap(short, long, default_value_t = String::from("xml"))]
    format: String,

    /// Export to json files
    #[clap(short, long)]
    json: Option<String>,

    /// Export to SQLite
    #[clap(short, long)]
    sqlite: Option<String>,
    
    /// Number of records in an import chunk
    #[clap(short, long, default_value_t = 1)]
    chunksize: usize,
}

fn main() {

    let args:Args = Args::parse();
    
    let input:&str = &args.input;
    let format:&str = &args.format;
    println!("file:   {}", input);
    println!("format: {}", format);
    //    println!("{}",create_table_sqlite());
    println!("{}",insert_sqlite(&Object::new()));
    match format {
        "xml" => {
            println!("READ XML {}", input);
            do_read(
                &mut File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input)),
                args.json,
                args.sqlite,
                args.chunksize
            )
        },

        "zip" => {
            println!("READ ZIP {}", input);
            let f = File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input));
            let mut archive = Box::new(zip::ZipArchive::new(f).unwrap_or_else(|err| panic!("{}\nCan't open zip archive {}", err, input)));
            let mut read = archive.by_index(0).unwrap_or_else(|err| panic!("{}\nCan't open zipped file {}", err, input)); 
            do_read(
                &mut read,
                args.json,
                args.sqlite,
                args.chunksize
            )
        }
        _ => panic!("Unsupported format: {}", format)
    };
}
