/*
 * pg_stub.c — Stub definitions for PostgreSQL server symbols.
 *
 * On macOS 26+ (Tahoe), dyld eagerly resolves all flat namespace symbols at
 * load time.  pgrx extensions reference ~28 PostgreSQL server-internal symbols
 * (MemoryContexts, SPI functions, etc.) that are normally provided by the
 * `postgres` executable when the extension is loaded as a shared library.
 *
 * When we run `cargo test --lib` to execute pure-Rust unit tests, there is no
 * postgres process — so those symbols are undefined and dyld aborts with:
 *
 *     dyld: symbol not found in flat namespace '_CacheMemoryContext'
 *
 * This stub provides NULL / no-op definitions for every PostgreSQL symbol the
 * test binary references.  It is compiled into `libpg_stub.dylib` and injected
 * via DYLD_INSERT_LIBRARIES when running unit tests.
 *
 * IMPORTANT: None of these stubs are ever _called_ during unit tests — the
 * tests exercise pure Rust logic only.  If a test accidentally calls a PG
 * function it will get a NULL pointer / zero return, which will crash or
 * fail the test (the desired behaviour).
 *
 * Regenerate the symbol list with:
 *   nm target/debug/deps/pg_trickle-* | grep ' U _' | awk '{print $NF}' \
 *     | grep -E '^_(Alloc|Cache|Copy|Cur|Current|err|Error|format_type|Free|Get|get_|Is|Mem|Message|palloc|palloc_|pfree|PG_|parse_|pg_|Portal|Postmaster|raw_|repalloc|SPI_|Top)'
 */

#include <stddef.h>
#include <stdint.h>

/* ── MemoryContext globals (all NULL) ─────────────────────────────────── */
void *CacheMemoryContext        = NULL;
void *CurrentMemoryContext      = NULL;
void *CurTransactionContext     = NULL;
void *ErrorContext              = NULL;
void *MessageContext            = NULL;
void *PortalContext             = NULL;
void *PostmasterContext         = NULL;
void *TopMemoryContext          = NULL;
void *TopTransactionContext     = NULL;

/* ── Error handling globals ───────────────────────────────────────────── */
void *error_context_stack       = NULL;
void *PG_exception_stack        = NULL;

/* ── Memory allocation functions ──────────────────────────────────────── */
void *palloc(size_t size)                       { (void)size; return NULL; }
void *palloc0(size_t size)                      { (void)size; return NULL; }
void *palloc_extended(size_t size, int flags)   { (void)size; (void)flags; return NULL; }
void *repalloc(void *pointer, size_t size)      { (void)pointer; (void)size; return NULL; }

/* ── MemoryContext functions ──────────────────────────────────────────── */
void *AllocSetContextCreateInternal(void *parent, const char *name,
                                    size_t minContextSize,
                                    size_t initBlockSize,
                                    size_t maxBlockSize) {
    (void)parent; (void)name;
    (void)minContextSize; (void)initBlockSize; (void)maxBlockSize;
    return NULL;
}

void  MemoryContextDelete(void *ctx)            { (void)ctx; }
void *MemoryContextGetParent(void *ctx)         { (void)ctx; return NULL; }
void  pfree(void *ptr)                          { (void)ptr; }

/* ── List helpers ─────────────────────────────────────────────────────── */
void *lappend(void *list, void *datum)          { (void)datum; return list; }
void *lcons(void *datum, void *list)            { (void)datum; return list; }
void  list_free(void *list)                     { (void)list; }
void  list_free_deep(void *list)                { (void)list; }

/* ── Error data ──────────────────────────────────────────────────────── */
void *CopyErrorData(void)                       { return NULL; }
void  FreeErrorData(void *edata)                { (void)edata; }

/* ── Error reporting functions ───────────────────────────────────────── */
int   errcode(int sqlerrcode)                   { (void)sqlerrcode; return 0; }
int   errmsg(const char *fmt, ...)              { (void)fmt; return 0; }
int   errdetail(const char *fmt, ...)           { (void)fmt; return 0; }
int   errhint(const char *fmt, ...)             { (void)fmt; return 0; }
int   errcontext_msg(const char *fmt, ...)      { (void)fmt; return 0; }
int   errstart(int elevel, const char *domain)  { (void)elevel; (void)domain; return 0; }
void  errfinish(const char *filename, int lineno, const char *funcname) {
    (void)filename; (void)lineno; (void)funcname;
}

/* ── Transaction / type helpers ───────────────────────────────────────── */
uint32_t MyDatabaseId                     = 0;
int      GetDatabaseEncoding(void)             { return 0; }
uint32_t GetOuterUserId(void)                  { return 0; }
void    *get_config_handle(const char *name)   { (void)name; return NULL; }
void    *find_option(const char *name, _Bool missing_ok, _Bool is_assign,
                     int elevel) {
    (void)name; (void)missing_ok; (void)is_assign; (void)elevel;
    return NULL;
}
int      set_config_option(const char *name, const char *value,
                           unsigned int context, unsigned int source,
                           unsigned int action, _Bool change_val,
                           int elevel, _Bool is_reload) {
    (void)name; (void)value; (void)context; (void)source; (void)action;
    (void)change_val; (void)elevel; (void)is_reload;
    return 0;
}
uint32_t GetCurrentTransactionId(void)          { return 0; }
uint32_t GetCurrentTransactionIdIfAny(void)     { return 0; }
uint32_t GetUserId(void)                         { return 0; }
void     GetUserIdAndSecContext(uint32_t *userid, int *sec_context) {
    if (userid) *userid = 0;
    if (sec_context) *sec_context = 0;
}
void     SetUserIdAndSecContext(uint32_t userid, int sec_context) {
    (void)userid;
    (void)sec_context;
}
int16_t  get_typlen(uint32_t typid)            { (void)typid; return -1; }
_Bool    get_typbyval(uint32_t typid)          { (void)typid; return 0; }
void     get_typlenbyval(uint32_t typid, int16_t *typlen, _Bool *typbyval) {
    (void)typid;
    if (typlen) {
        *typlen = -1;
    }
    if (typbyval) {
        *typbyval = 0;
    }
}
void     get_typlenbyvalalign(uint32_t typid, int16_t *typlen,
                              _Bool *typbyval, char *typalign) {
    (void)typid;
    if (typlen) {
        *typlen = -1;
    }
    if (typbyval) {
        *typbyval = 0;
    }
    if (typalign) {
        *typalign = 'i';
    }
}
uint32_t get_array_type(uint32_t typid)         { (void)typid; return 0; }
int   IsBinaryCoercible(uint32_t a, uint32_t b) { (void)a; (void)b; return 0; }
char *format_type_extended(uint32_t oid, int32_t typmod, int flags) {
    (void)oid; (void)typmod; (void)flags;
    return NULL;
}

/* ── Toast helpers ────────────────────────────────────────────────────── */
void *pg_detoast_datum(void *datum)             { return datum; }
void *pg_detoast_datum_copy(void *datum)        { return datum; }
void *pg_detoast_datum_slice(void *datum, int32_t first, int32_t count) {
    (void)first;
    (void)count;
    return datum;
}
void *pg_detoast_datum_packed(void *datum)      { return datum; }

/* ── Type output helpers ──────────────────────────────────────────────── */
uintptr_t byteaout(void *fcinfo)                { (void)fcinfo; return 0; }
uintptr_t textout(void *fcinfo)                 { (void)fcinfo; return 0; }
uintptr_t json_out(void *fcinfo)                { (void)fcinfo; return 0; }
uintptr_t jsonb_out(void *fcinfo)               { (void)fcinfo; return 0; }

/* ── Parser entry points ─────────────────────────────────────────────── */
void *raw_parser(const char *str, int mode) {
    (void)str; (void)mode;
    return NULL;
}

void *parse_analyze_fixedparams(void *parse_tree,
                                const char *source_text,
                                const uint32_t *param_types,
                                int num_params,
                                void *query_env) {
    (void)parse_tree; (void)source_text;
    (void)param_types; (void)num_params; (void)query_env;
    return NULL;
}

/* ── SPI functions ───────────────────────────────────────────────────── */
int      SPI_connect(void)                      { return -1; }
int      SPI_finish(void)                       { return -1; }
int      SPI_execute(const char *cmd, int ro, long cnt) {
    (void)cmd; (void)ro; (void)cnt;
    return -1;
}
int      SPI_execute_with_args(const char *cmd, int nargs,
                               void *argtypes, void *values,
                               const char *nulls, int ro, long cnt) {
    (void)cmd; (void)nargs; (void)argtypes; (void)values;
    (void)nulls; (void)ro; (void)cnt;
    return -1;
}
int      SPI_fnumber(void *tupdesc, const char *fname) {
    (void)tupdesc; (void)fname;
    return -1;
}
char    *SPI_fname(void *tupdesc, int fnumber) {
    (void)tupdesc; (void)fnumber;
    return NULL;
}
char    *SPI_getvalue(void *tuple, void *tupdesc, int fnumber) {
    (void)tuple; (void)tupdesc; (void)fnumber;
    return NULL;
}
void    *SPI_getbinval(void *tuple, void *tupdesc, int fnumber,
                       int *isnull) {
    (void)tuple; (void)tupdesc; (void)fnumber;
    if (isnull) *isnull = 1;
    return NULL;
}
char    *SPI_gettype(void *tupdesc, int fnumber) {
    (void)tupdesc; (void)fnumber;
    return NULL;
}
uint32_t SPI_gettypeid(void *tupdesc, int fnumber) {
    (void)tupdesc; (void)fnumber;
    return 0;
}

/* SPI globals */
uint64_t SPI_processed = 0;
void    *SPI_tuptable  = NULL;

/* ── LWLock stubs ────────────────────────────────────────────────────── */
/*
 * pgrx's PgLwLock wrapper calls LWLockAcquire/LWLockRelease.  The symbols
 * are referenced in the test binary even though unit tests never actually
 * acquire a lock.  On Linux the dynamic linker resolves all undefined
 * symbols at load time (RELRO / -z now), so without these stubs the
 * process exits with "undefined symbol: LWLockAcquire" before any test
 * runs.
 */
int   LWLockAcquire(void *lock, int mode) { (void)lock; (void)mode; return 1; }
void  LWLockRelease(void *lock)            { (void)lock; }
void *GetNamedLWLockTranche(const char *name) { (void)name; return NULL; }
void  RequestNamedLWLockTranche(const char *name, int num_tranches) {
    (void)name; (void)num_tranches;
}

/* MainLWLockArray is a global array of LWLocks; the test binary references
 * its address.  A single NULL pointer is enough so the loader is satisfied. */
void *MainLWLockArray = NULL;

/* ── Interrupt / signal globals ──────────────────────────────────────── */
/*
 * pgrx references InterruptHoldoffCount in generated code.  Unit tests
 * never trigger interrupts, but the symbol must exist for the loader.
 */
int InterruptHoldoffCount = 0;

/* ── Array accumulation ───────────────────────────────────────────────── */
/*
 * pgrx-generated code for ARRAY return types references these functions.
 */
void *initArrayResult(uint32_t element_type, void *rcontext, int subcontext) {
    (void)element_type; (void)rcontext; (void)subcontext; return NULL;
}
void *accumArrayResult(void *astate, uintptr_t dvalue, int disnull,
                       uint32_t element_type, void *rcontext) {
    (void)astate; (void)dvalue; (void)disnull;
    (void)element_type; (void)rcontext; return NULL;
}
uintptr_t makeArrayResult(void *astate, void *rcontext) {
    (void)astate; (void)rcontext; return 0;
}

/* ── Query / expression tree walkers ─────────────────────────────────── */
int expression_tree_walker_impl(void *node, void *walker, void *context,
                                int flags) {
    (void)node; (void)walker; (void)context; (void)flags; return 0;
}
int raw_expression_tree_walker_impl(void *node, void *walker, void *context) {
    (void)node; (void)walker; (void)context; return 0;
}
int query_tree_walker_impl(void *query, void *walker, void *context,
                           int flags) {
    (void)query; (void)walker; (void)context; (void)flags; return 0;
}
uint32_t exprType(const void *expr) { (void)expr; return 0; }

/* ── jsonb input ──────────────────────────────────────────────────────── */
uintptr_t jsonb_in(void *fcinfo) { (void)fcinfo; return 0; }

/* ── pgrx 0.18.0: message level filter ───────────────────────────────── */
/*
 * pgrx 0.18.0 calls message_level_is_interesting() before emitting any log
 * message.  Return 1 (true) so that pgrx::error!() correctly triggers the
 * panic path (panic_any) in unit tests.  Lower-level messages (WARNING,
 * INFO, etc.) then try do_ereport(), but our errstart() stub returns 0
 * (false) so the actual message body is never built.
 */
int message_level_is_interesting(int elevel) { (void)elevel; return 1; }

/* ── Error state helpers ──────────────────────────────────────────────── */
/*
 * FlushErrorState is referenced by pgrx 0.18.0 error-recovery paths.
 * In unit tests, error recovery never actually runs; we just need the
 * symbol to satisfy the dynamic linker.
 */
void FlushErrorState(void) {}

/* ── Crypto stubs ─────────────────────────────────────────────────────── */
/*
 * CCRandomGenerateBytes is a macOS Security framework function referenced
 * by some pgrx random-number helpers.  Return success (0) with zeroed bytes.
 */
int CCRandomGenerateBytes(void *bytes, size_t count) {
    if (bytes && count > 0) {
        __builtin_memset(bytes, 0, count);
    }
    return 0;
}

/* ── sigsetjmp stub ──────────────────────────────────────────────────── */
/*
 * pgrx's PG_exception_stack references sigsetjmp indirectly. On macOS the
 * symbol is provided by libSystem so it should always resolve, but we
 * include it here defensively.
 */
