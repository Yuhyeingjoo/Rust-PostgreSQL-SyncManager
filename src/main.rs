extern crate ini;
extern crate postgres;
use postgres::{Client, NoTls, Error};
use std::sync::atomic::{AtomicBool, Ordering};
use std::env;
use ini::Ini;

#[derive(Debug)]
struct DbInfo {
    ip: String,
    user: String,
    dbname: String,
    password: String,
}

static USE_PRIMARY_DB: AtomicBool = AtomicBool::new(true);
fn read_db_info(config: &Ini, section: &str) -> DbInfo {
    let db_section = config.section(Some(section.to_owned())).expect(&format!("{} section not found in config.ini", section));

    let ip = db_section.get("ip").expect("IP not found in config.ini").to_owned();
    let user = db_section.get("user").expect("Username not found in config.ini").to_owned();
    let dbname = db_section.get("dbname").expect("DB name not found in config.ini").to_owned();
    let password = db_section.get("password").expect("Password not found in config.ini").to_owned();

    DbInfo { ip, user, dbname, password }
}

fn connect_to_database(info: &DbInfo) -> Result<Client, postgres::Error> {
    let conn_str = format!("host={} user={} dbname={} password={}", info.ip, info.user, info.dbname, info.password);
    let client = Client::connect(conn_str.as_str(), NoTls)?;
    Ok(client)
}
fn execute_query(client: &mut Client, query: &str) -> Result<(), Error> {
    client.batch_execute(query)?;
    Ok(())
}

fn execute_select_query(client: &mut Client, query: &str) -> Result<Vec<String>, Error> {
    let rows = client.query(query, &[])?;
    let mut results = Vec::new();
    for row in rows {
        results.push(row.get::<usize, String>(0));
    }
    Ok(results)
}

fn clean_query(query: &str) -> String {
    query.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("--"))
        .collect::<Vec<&str>>()
        .join(" ")
}

fn is_select_query(query: &str) -> bool {
    let cleaned_query = clean_query(query);
    cleaned_query.to_lowercase().starts_with("select")
}

fn is_dml_query(query: &str) -> bool {
    let cleaned_query = clean_query(query);
    cleaned_query.to_lowercase().starts_with("insert") ||
    cleaned_query.to_lowercase().starts_with("update") ||
    cleaned_query.to_lowercase().starts_with("delete")
}

fn synchronize_sql_operation(
    current_client: &mut Client,
    replica_client: &mut Client,
    sql: &str
) -> Result<(), Error> {
    if is_dml_query(sql) {
        execute_query(current_client, sql)?;
        execute_query(replica_client, sql)?;
        println!("DML query executed successfully on both databases.");
    } else if is_select_query(sql) {
        let use_primary = USE_PRIMARY_DB.load(Ordering::SeqCst);
        let result = if use_primary {
            execute_select_query(current_client, sql)
        } else {
            execute_select_query(replica_client, sql)
        };

        USE_PRIMARY_DB.store(!use_primary, Ordering::SeqCst); // Toggle the boolean

        match result {
            Ok(res) => println!("SELECT query executed. Results: {:?}", res),
            Err(e) => eprintln!("Error executing SELECT query: {}", e),
        }
    } else {
        println!("Unsupported query type.");
    }
    Ok(())
}


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

    let mut  current_db_client = connect_to_database(&current_db_info)
        .expect("Failed to connect to current database");
    let result = current_db_client.query("SELECT 1", &[]).expect("Failed to execute query");

    for row in result {
        let value: i32 = row.get(0);
        println!("Connection to current database successful: {}", value);
    }
    let mut  replica_db_client = connect_to_database(&replica_db_info)
        .expect("Failed to connect to replica database");

    let sql = "INSERT INTO table_name (column1, column2) VALUES ('value1', 'value2')";

   
    if let Err(err) = synchronize_sql_operation(&mut current_db_client, &mut replica_db_client, sql) {
        eprintln!("Failed to synchronize SQL operation: {}", err);
    }
    
}
