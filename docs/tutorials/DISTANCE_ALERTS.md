# Distance alerts

`pgtrickle.subscribe_distance()` sends a PostgreSQL notification when rows in
a stream table enter or leave a distance region after a successful refresh.
The caller must own the stream table (or be a superuser). For example:

```sql
SELECT pgtrickle.subscribe_distance(
    'public.embedding_store', 'near_documents', 'embedding',
    '[1,0,0]', '<=>', 0.2
);
LISTEN near_documents;
```

Use a query vector with the subscribed column's type and dimensions. The
operator must be provided by the installed pgvector extension for that type:
`<->` is L2 distance, `<=>` is cosine distance, `<#>` is **negative** inner
product, `<+>` is L1 distance, `<~>` is Hamming distance, and `<%>` is Jaccard
distance. Not every operator supports every type. Thresholds must be finite;
for example, an inner product greater than 0.8 corresponds to `<#> -0.8`.

Registration and subscription edits establish a baseline from the current
stream contents and do not themselves send an event. A successful refresh
reports net changes by stream row identity: a key change can report one exit
and one entry, while a row that remains inside the region reports neither.
The JSON payload includes `schema`, `stream_table`, `op`, `threshold`,
`entered`, `exited`, and `refresh_id`. It does not contain embeddings or source
documents. An aborted transaction delivers no notification.

Notifications are best-effort wake-ups, not durable delivery. PostgreSQL
channels are not tenant access controls; do not put secrets in channel names.
Subscribers needing durable changes should use the separately documented
[output-delta consumer API](../SQL_REFERENCE.md). Evaluation errors are
logged and do not abort the refresh.

Ordinary row-wise differential refreshes compare affected row identities.
Other query shapes and FULL refreshes use a logged full snapshot. A protected
ALTER QUERY, partition, or refresh-mode rebuild compares the original and
replacement storage and records its FULL refresh in history when a distance
snapshot is available.
Direct repair operations do not currently emit crossing notifications; inspect
the stream contents after such operations. IMMEDIATE
mode and pooler compatibility mode do not support these notifications, and new
registrations in those modes are rejected. If pooler mode is enabled later,
refreshes log a warning instead of emitting alerts.

If a rebuild recursively re-enters the same stream before its snapshot is
consumed, the nested alert is skipped with a warning; the outer snapshot is
preserved.

The subscription tracks the stream table's catalog ID and is removed when the
stream table is dropped. Directly renaming the underlying storage table is not
a supported stream-table lifecycle operation; subscription IDs do not repair
the stream table's own catalog metadata after such a rename. The list function
shows subscriptions for tables the
caller owns; a superuser can see all bound subscriptions. During upgrade,
legacy subscriptions are retained inactive with a NULL `pgt_id` and reported
by a warning. Thresholds stored by builds affected by the floating-point
binding bug cannot be reconstructed reliably: an administrator must inspect
the legacy rows and re-register subscriptions with their intended thresholds.
