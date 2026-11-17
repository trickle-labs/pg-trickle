use postgres::{Client, Config, NoTls};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DumpFormat {
    Sql,
}

#[derive(Debug)]
struct DumpOptions {
    dsn: Option<String>,
    output: Option<PathBuf>,
    format: DumpFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamTableDumpRow {
    pgt_id: i64,
    schema_name: String,
    stream_name: String,
    query: String,
    schedule: String,
    refresh_mode: String,
    status: String,
    initialize: bool,
    diamond_consistency: String,
    diamond_schedule_policy: String,
}

impl StreamTableDumpRow {
    fn qualified_name(&self) -> String {
        format_qualified_name(&self.schema_name, &self.stream_name)
    }
}

fn print_usage() {
    eprintln!(
        "Usage: pg_trickle_dump [--dsn <connstr>] [--output <path>] [--format sql]\n\
         \n\
         Connection resolution order:\n\
         1. --dsn\n\
         2. DATABASE_URL\n\
         3. PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE environment variables\n"
    );
}

fn parse_args() -> Result<DumpOptions, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut dsn = None;
    let mut output = None;
    let mut format = DumpFormat::Sql;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dsn" => {
                dsn = Some(args.next().ok_or("missing value for --dsn")?);
            }
            "--output" => {
                output = Some(PathBuf::from(
                    args.next().ok_or("missing value for --output")?,
                ));
            }
            "--format" => {
                let value = args.next().ok_or("missing value for --format")?;
                format = match value.as_str() {
                    "sql" => DumpFormat::Sql,
                    other => {
                        return Err(format!("unsupported format: {other} (expected: sql)").into());
                    }
                };
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    Ok(DumpOptions {
        dsn,
        output,
        format,
    })
}

fn connect(options: &DumpOptions) -> Result<Client, Box<dyn Error>> {
    if let Some(dsn) = &options.dsn {
        return Ok(Client::connect(dsn, NoTls)?);
    }

    if let Ok(dsn) = env::var("DATABASE_URL") {
        return Ok(Client::connect(&dsn, NoTls)?);
    }

    let mut config = Config::new();
    let mut configured = false;

    if let Ok(host) = env::var("PGHOST") {
        config.host(&host);
        configured = true;
    }
    if let Ok(port) = env::var("PGPORT") {
        config.port(port.parse()?);
        configured = true;
    }
    if let Ok(user) = env::var("PGUSER") {
        config.user(&user);
        configured = true;
    }
    if let Ok(password) = env::var("PGPASSWORD") {
        config.password(password);
        configured = true;
    }
    if let Ok(dbname) = env::var("PGDATABASE") {
        config.dbname(&dbname);
        configured = true;
    }

    config.application_name("pg_trickle_dump");

    if !configured {
        config.host("localhost");
        config.user("postgres");
        config.dbname("postgres");
    }

    Ok(config.connect(NoTls)?)
}

fn load_stream_tables(client: &mut Client) -> Result<Vec<StreamTableDumpRow>, Box<dyn Error>> {
    let rows = client.query(
        "SELECT
             pgt_id,
             pgt_schema,
             pgt_name,
             COALESCE(original_query, defining_query) AS restore_query,
             COALESCE(schedule, 'calculated') AS restore_schedule,
             refresh_mode,
             status,
             is_populated,
             diamond_consistency,
             diamond_schedule_policy
         FROM pgtrickle.pgt_stream_tables
         ORDER BY pgt_schema, pgt_name",
        &[],
    )?;

    Ok(rows
        .into_iter()
        .map(|row| StreamTableDumpRow {
            pgt_id: row.get("pgt_id"),
            schema_name: row.get("pgt_schema"),
            stream_name: row.get("pgt_name"),
            query: row.get("restore_query"),
            schedule: row.get("restore_schedule"),
            refresh_mode: row.get("refresh_mode"),
            status: row.get("status"),
            initialize: row.get("is_populated"),
            diamond_consistency: row.get("diamond_consistency"),
            diamond_schedule_policy: row.get("diamond_schedule_policy"),
        })
        .collect())
}

fn load_stream_table_edges(client: &mut Client) -> Result<Vec<(i64, i64)>, Box<dyn Error>> {
    let rows = client.query(
        "SELECT child.pgt_id AS child_id, parent.pgt_id AS parent_id
         FROM pgtrickle.pgt_dependencies dep
         JOIN pgtrickle.pgt_stream_tables child ON child.pgt_id = dep.pgt_id
         JOIN pgtrickle.pgt_stream_tables parent ON parent.pgt_relid = dep.source_relid
         WHERE dep.source_type = 'STREAM_TABLE'",
        &[],
    )?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get("parent_id"), row.get("child_id")))
        .collect())
}

fn topo_sort_stream_tables(
    stream_tables: &[StreamTableDumpRow],
    edges: &[(i64, i64)],
) -> Result<Vec<StreamTableDumpRow>, Box<dyn Error>> {
    let mut by_id: BTreeMap<i64, StreamTableDumpRow> = stream_tables
        .iter()
        .cloned()
        .map(|row| (row.pgt_id, row))
        .collect();
    let mut indegree: BTreeMap<i64, usize> = by_id.keys().map(|id| (*id, 0usize)).collect();
    let mut adjacency: BTreeMap<i64, Vec<i64>> = BTreeMap::new();

    for &(parent, child) in edges {
        if by_id.contains_key(&parent) && by_id.contains_key(&child) {
            adjacency.entry(parent).or_default().push(child);
            *indegree.entry(child).or_default() += 1;
        }
    }

    let mut ready: VecDeque<i64> = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect();

    let mut ready_sorted: Vec<i64> = ready.drain(..).collect();
    ready_sorted.sort_by_key(|id| {
        by_id
            .get(id)
            .map(StreamTableDumpRow::qualified_name)
            .unwrap_or_default()
    });
    ready = ready_sorted.into();

    let mut ordered = Vec::with_capacity(by_id.len());

    while let Some(id) = ready.pop_front() {
        let row = by_id.remove(&id).ok_or("topological sort lost node")?;
        ordered.push(row);

        let mut newly_ready = Vec::new();
        if let Some(children) = adjacency.get(&id) {
            for child in children {
                if let Some(degree) = indegree.get_mut(child) {
                    *degree -= 1;
                    if *degree == 0 {
                        newly_ready.push(*child);
                    }
                }
            }
        }
        newly_ready.sort_by_key(|child_id| {
            by_id
                .get(child_id)
                .map(StreamTableDumpRow::qualified_name)
                .unwrap_or_default()
        });
        for child in newly_ready {
            ready.push_back(child);
        }
    }

    if ordered.len() != stream_tables.len() {
        return Err("stream table dependency graph contains a cycle".into());
    }

    Ok(ordered)
}

fn sql_literal(input: &str) -> String {
    format!("'{}'", input.replace('\'', "''"))
}

fn format_identifier(ident: &str) -> String {
    let needs_quotes = ident.is_empty()
        || !ident.chars().enumerate().all(|(index, ch)| match index {
            0 => ch == '_' || ch.is_ascii_lowercase(),
            _ => ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit(),
        });

    if needs_quotes {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        ident.to_string()
    }
}

fn format_qualified_name(schema_name: &str, stream_name: &str) -> String {
    format!(
        "{}.{}",
        format_identifier(schema_name),
        format_identifier(stream_name)
    )
}

fn dollar_quote(input: &str) -> String {
    let mut index = 0usize;
    loop {
        let tag = if index == 0 {
            "$pgt$".to_string()
        } else {
            format!("$pgt{index}$")
        };
        if !input.contains(&tag) {
            return format!("{tag}{input}{tag}");
        }
        index += 1;
    }
}

fn generate_restore_sql(stream_tables: &[StreamTableDumpRow]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "-- pg_trickle_dump SQL export");
    let _ = writeln!(out, "-- Generated at {}", jiff::Timestamp::now());
    let _ = writeln!(out, "-- Stream tables exported: {}", stream_tables.len());
    let _ = writeln!(
        out,
        "-- Requires CREATE EXTENSION pg_trickle; before replay."
    );
    let _ = writeln!(out);

    if stream_tables.is_empty() {
        let _ = writeln!(out, "-- No stream tables found.");
        return out;
    }

    for stream_table in stream_tables {
        let qualified_name = stream_table.qualified_name();
        let _ = writeln!(out, "-- Recreate {qualified_name}");
        let _ = writeln!(
            out,
            "SELECT pgtrickle.create_stream_table(\n    {},\n    {},\n    schedule => {},\n    refresh_mode => {},\n    initialize => {},\n    diamond_consistency => {},\n    diamond_schedule_policy => {}\n);",
            sql_literal(&qualified_name),
            dollar_quote(&stream_table.query),
            sql_literal(&stream_table.schedule),
            sql_literal(&stream_table.refresh_mode),
            if stream_table.initialize {
                "true"
            } else {
                "false"
            },
            sql_literal(&stream_table.diamond_consistency),
            sql_literal(&stream_table.diamond_schedule_policy),
        );

        if stream_table.status != "ACTIVE" {
            let _ = writeln!(
                out,
                "SELECT pgtrickle.alter_stream_table({}, status => {});",
                sql_literal(&qualified_name),
                sql_literal(&stream_table.status),
            );
        }

        let _ = writeln!(out);
    }

    out
}

fn write_output(options: &DumpOptions, contents: &str) -> Result<(), Box<dyn Error>> {
    if let Some(path) = &options.output {
        fs::write(path, contents)?;
    } else {
        print!("{contents}");
    }
    Ok(())
}

fn run() -> Result<(), Box<dyn Error>> {
    let options = parse_args()?;
    match options.format {
        DumpFormat::Sql => {
            let mut client = connect(&options)?;
            let stream_tables = load_stream_tables(&mut client)?;
            let edges = load_stream_table_edges(&mut client)?;
            let ordered = topo_sort_stream_tables(&stream_tables, &edges)?;
            let sql = generate_restore_sql(&ordered);
            write_output(&options, &sql)?;
        }
    }

    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("pg_trickle_dump: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topo_sort_stream_tables_orders_dependencies() {
        let stream_tables = vec![
            StreamTableDumpRow {
                pgt_id: 2,
                schema_name: "public".to_string(),
                stream_name: "downstream".to_string(),
                query: "select * from upstream".to_string(),
                schedule: "calculated".to_string(),
                refresh_mode: "FULL".to_string(),
                status: "ACTIVE".to_string(),
                initialize: true,
                diamond_consistency: "atomic".to_string(),
                diamond_schedule_policy: "fastest".to_string(),
            },
            StreamTableDumpRow {
                pgt_id: 1,
                schema_name: "public".to_string(),
                stream_name: "upstream".to_string(),
                query: "select 1".to_string(),
                schedule: "1m".to_string(),
                refresh_mode: "FULL".to_string(),
                status: "ACTIVE".to_string(),
                initialize: true,
                diamond_consistency: "atomic".to_string(),
                diamond_schedule_policy: "fastest".to_string(),
            },
        ];

        let ordered = topo_sort_stream_tables(&stream_tables, &[(1, 2)]).unwrap();
        assert_eq!(
            ordered
                .iter()
                .map(|row| row.qualified_name())
                .collect::<Vec<_>>(),
            vec!["public.upstream", "public.downstream"]
        );
    }

    #[test]
    fn test_generate_restore_sql_restores_non_active_status() {
        let sql = generate_restore_sql(&[StreamTableDumpRow {
            pgt_id: 7,
            schema_name: "public".to_string(),
            stream_name: "orders_st".to_string(),
            query: "SELECT * FROM orders".to_string(),
            schedule: "calculated".to_string(),
            refresh_mode: "DIFFERENTIAL".to_string(),
            status: "SUSPENDED".to_string(),
            initialize: false,
            diamond_consistency: "atomic".to_string(),
            diamond_schedule_policy: "fastest".to_string(),
        }]);

        assert!(sql.contains("SELECT pgtrickle.create_stream_table("));
        assert!(sql.contains("initialize => false"));
        assert!(sql.contains(
            "SELECT pgtrickle.alter_stream_table('public.orders_st', status => 'SUSPENDED');"
        ));
    }

    #[test]
    fn test_dollar_quote_chooses_non_conflicting_tag() {
        let quoted = dollar_quote("SELECT '$pgt$' AS marker");
        assert!(quoted.starts_with("$pgt1$"));
        assert!(quoted.ends_with("$pgt1$"));
    }

    #[test]
    fn test_format_qualified_name_quotes_non_standard_identifiers() {
        assert_eq!(
            format_qualified_name("CamelCase", "order.items"),
            "\"CamelCase\".\"order.items\""
        );
    }
}
