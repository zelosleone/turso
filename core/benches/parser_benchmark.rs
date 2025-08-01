use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use turso_core::parser::{lexer::Lexer, parser::Parser};

fn bench_lexer(criterion: &mut Criterion) {
    let queries = [
        "SELECT 1",
        "SELECT * FROM users LIMIT 1",
        "SELECT first_name, count(1) FROM users GROUP BY first_name HAVING count(1) > 1 ORDER BY count(1)  LIMIT 1",
    ];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Lexer `{query}`"));
        let qb = query.as_bytes();

        group.bench_function(BenchmarkId::new("limbo_lexer_query", ""), |b| {
            b.iter(|| {
                for token in Lexer::new(black_box(qb)) {
                    token.unwrap();
                }
            });
        });

        group.finish();
    }
}

fn bench_parser(criterion: &mut Criterion) {
    let queries = ["BEGIN", "BEGIN EXCLUSIVE TRANSACTION my_trans"];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Parser `{query}`"));
        let qb = query.as_bytes();

        group.bench_function(BenchmarkId::new("limbo_parser_query", ""), |b| {
            b.iter(|| {
                for stmt in Parser::new(black_box(qb)) {
                    stmt.unwrap();
                }
            });
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_lexer, bench_parser
}
criterion_main!(benches);
