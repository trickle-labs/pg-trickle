use std::{hint::black_box, time::Instant};

fn current(prefix: &str, count: usize) -> String {
    let suffix = format!("_{count}");
    let max_prefix = 63usize.saturating_sub("__pgt_cte_".len() + suffix.len());
    let safe_prefix: String = prefix.chars().take(max_prefix).map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }
    }).collect();
    format!("__pgt_cte_{safe_prefix}{suffix}")
}

fn proposed(prefix: &str, count: usize) -> String {
    let suffix = format!("_{count}");
    let max_prefix = 63usize.saturating_sub("__pgt_cte_".len() + suffix.len());
    let mut result = String::with_capacity(63);
    result.push_str("__pgt_cte_");
    result.extend(prefix.chars().take(max_prefix).map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }
    }));
    result.push_str(&suffix);
    result
}

fn main() {
    let prefixes = ["agg_delta", "agg_merge", "agg_final", "Doc.Source \"λ\"", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789long"];
    for p in prefixes { for n in [1, 999, usize::MAX] { assert_eq!(current(p,n),proposed(p,n)); } }
    for (label, f) in [("current", current as fn(&str, usize)->String), ("proposed", proposed), ("proposed", proposed), ("current", current)] {
        let start = Instant::now();
        for i in 0..2_000_000 { black_box(f(black_box(prefixes[i%3]), black_box(i%10+1))); }
        println!("{label}: {:.2} ns/call", start.elapsed().as_nanos() as f64 / 2_000_000.0);
    }
}
