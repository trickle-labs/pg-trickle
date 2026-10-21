# Files

- [Source capture, buffers, and safe frontiers](change-capture.md) - How pg_trickle captures transactional source changes, bounds refresh visibility, and retains shared buffers. Covers trigger and polling ownership, optional receipt-backed WAL capture, and crash recovery boundaries.
- [Privilege, execution-context, and error boundaries](security-boundaries.md) - How privileged lifecycle and refresh code accesses private state while executing definition-derived SQL as the stream owner. Covers caller capture, delegated refresh authority, RLS revalidation, SQL construction, and PostgreSQL error cleanup.
- [Stream table lifecycle and dependency ownership](stream-table-lifecycle.md) - How SQL lifecycle operations coordinate stream-table storage, durable metadata, dependency graphs, capture resources, and refresh state. Explains transaction boundaries, protected query replacement, graph admission, and last-consumer cleanup.
