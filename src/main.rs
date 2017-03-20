extern crate clap;
extern crate zip;
extern crate xml;

use clap::{Arg, App};
use std::io::{BufReader, Read};
use std::fs::File;
use std::str;
use xml::reader::{EventReader, XmlEvent};

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
        Record{
        element: String::from(element),
        is_struct: is_struct(element),
        text: String::new(),
        structure: Vec::new()
        }
    }
    fn add_text(&mut self, text:&str){
        self.text.push_str(text);
    }
    fn add_child(&mut self, rec:Record){
        self.structure.push(rec);
    }
}

fn do_read(r: &mut Read) {
    let file = BufReader::new(r);
    let parser = EventReader::new(file);
    let mut stack:Vec<Record> = Vec::new();
    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
                println!("Start {}", name.local_name);
                if !stack.is_empty() || name.local_name == "Statistik"{
                    stack.push(Record::new(&name.local_name));
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
                println!("End   {}", name);
                if let Some(mut rec) = stack.pop(){
                    if stack.is_empty(){
                        println!("--> {:?}",rec);
                    }
                    else{
                        if let Some(mut parent) = stack.pop(){
                            parent.add_child(rec);
                            stack.push(parent);
                        }
                    }
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                println!("Text  {}", text);

                if let Some(mut rec) = stack.pop(){
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
        .get_matches();

    let input = matches.value_of("input").unwrap_or("ESStatistikListeModtag.xml");
    let format = matches.value_of("format").unwrap_or("xml");
    println!("file:   {}", input);
    println!("format: {}", format);
    match format {
        "xml" => {
            println!("READ XML {}", input);
            do_read(&mut File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input)))
        },

        "zip" => {
            println!("READ ZIP {}", input);
            let f = File::open(input).unwrap_or_else(|err| panic!("{}\nCan't open file {}", err, input));
            let mut archive = Box::new(zip::ZipArchive::new(f).unwrap_or_else(|err| panic!("{}\nCan't open zip archive {}", err, input)));
            do_read(&mut archive.by_index(0).unwrap_or_else(|err| panic!("{}\nCan't open zipped file {}", err, input)))
        }
        _ => panic!("Unsupported format: {}", format)
    };
}
