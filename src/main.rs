extern crate ini;
extern crate postgres;
use std::env;
use ini::Ini;
use postgres::{Client, NoTls};

// DB 연결 정보 구조체
#[derive(Debug)]
struct DbInfo {
    ip: String,
    user: String,
    dbname: String,
    password: String,
}

// INI 파일에서 DB 정보를 읽어오는 함수
fn read_db_info(config: &Ini, section: &str) -> DbInfo {
    let db_section = config.section(Some(section.to_owned())).expect(&format!("{} section not found in config.ini", section));

    let ip = db_section.get("ip").expect("IP not found in config.ini").to_owned();
    let user = db_section.get("user").expect("Username not found in config.ini").to_owned();
    let dbname = db_section.get("dbname").expect("DB name not found in config.ini").to_owned();
    let password = db_section.get("password").expect("Password not found in config.ini").to_owned();

    DbInfo { ip, user, dbname, password }
}

// DB에 연결하는 함수
fn connect_to_database(info: &DbInfo) -> Result<Client, postgres::Error> {
    let conn_str = format!("host={} user={} dbname={} password={}", info.ip, info.user, info.dbname, info.password);
    let client = Client::connect(conn_str.as_str(), NoTls)?;
    Ok(client)
}

// 현재 DB에서 실행하는 SQL 작업을 다른 DB에 동기화하는 함수
fn synchronize_sql_operation(current_client: &mut Client, other_client: &mut Client, sql: &str) -> Result<(), postgres::Error> {

    current_client.execute(sql, &[])?;

    // 다른 DB에 SQL을 실행
    other_client.execute(sql, &[])?;

    Ok(())
}

// 메인 함수
fn main() {
    let args: Vec<String> = env::args().collect();
     if args.len() < 2 {
        eprintln!("Usage: cargo run <config_file>");
        std::process::exit(1);
    }
    let config_file_path = &args[1];
    
    let config = ini::Ini::load_from_file(config_file_path).expect("Failed to load config.ini");
    let current_db_info = read_db_info(&config, "current_db");
    let replica_db_info = read_db_info(&config, "replica_db");

    // 현재 DB에 연결
    let mut  current_db_client = connect_to_database(&current_db_info)
        .expect("Failed to connect to current database");
    let result = current_db_client.query("SELECT 1", &[]).expect("Failed to execute query");

    // 결과 출력
    for row in result {
        let value: i32 = row.get(0);
        println!("Connection to current database successful: {}", value);
    }
    /*
    // 다른 DB에 연결
    let mut  replica_db_client = connect_to_database(&replica_db_info)
        .expect("Failed to connect to replica database");

    // 사용자가 입력한 SQL
    let sql = "INSERT INTO table_name (column1, column2) VALUES ('value1', 'value2')";

   
    // 현재 DB에서 SQL 실행하고 다른 DB와 동기화
    if let Err(err) = synchronize_sql_operation(&mut current_db_client, &mut replica_db_client, sql) {
        eprintln!("Failed to synchronize SQL operation: {}", err);
    }
    */
    
}
