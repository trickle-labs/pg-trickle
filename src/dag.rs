//! Dependency DAG construction, topological sort, and cycle detection.
//!
//! The DAG tracks relationships between stream tables and their upstream
//! sources (base tables, views, or other stream tables).
//!
//! # Prior Art — Scheduling Theory
//!
//! The earliest-deadline-first (EDF) scheduler in `scheduler.rs` follows the
//! classic EDF algorithm:
//! - Liu, C.L. & Layland, J.W. (1973). "Scheduling Algorithms for
//!   Multiprogramming in a Hard-Real-Time Environment." Journal of the ACM,
//!   20(1), 46–61.
//!   EDF is optimal for uniprocessor preemptive scheduling and is widely used
//!   in operating systems and real-time databases.
//!
//! # Prior Art — Graph Algorithms
//!
//! The dependency graph algorithms (topological sort, cycle detection) use
//! Kahn's algorithm:
//! - Kahn, A.B. (1962). "Topological sorting of large networks."
//!   Communications of the ACM, 5(11), 558–562.
//!   This is standard computer science curriculum and appears in every major
//!   algorithms textbook (Cormen et al., Sedgewick, Kleinberg & Tardos).

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use crate::error::PgTrickleError;

// ── Diamond dependency types ───────────────────────────────────────────────

/// Per-stream-table diamond consistency mode.
///
/// Controls whether a stream table participates in atomic SAVEPOINT-based
/// refresh groups when it is part of a diamond dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiamondConsistency {
    /// No atomic grouping — each ST refreshes independently (default).
    None,
    /// All members of the diamond's consistency group are wrapped in a
    /// single SAVEPOINT; if any member fails the entire group rolls back.
    Atomic,
}

impl DiamondConsistency {
    /// Serialize to the SQL CHECK constraint value.
    pub fn as_str(&self) -> &'static str {
        match self {
            DiamondConsistency::None => "none",
            DiamondConsistency::Atomic => "atomic",
        }
    }

    /// Deserialize from SQL string. Falls back to `None` for unknown values.
    pub fn from_sql_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "atomic" => DiamondConsistency::Atomic,
            _ => DiamondConsistency::None,
        }
    }
}

impl std::fmt::Display for DiamondConsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Per-convergence-node schedule policy for diamond consistency groups.
///
/// Controls whether a multi-member atomic group fires as soon as *any* member
/// is due (`Fastest`) or waits until *all* members are due (`Slowest`).
///
/// Set on the convergence node; the GUC `pg_trickle.diamond_schedule_policy`
/// provides a cluster-wide fallback.  When multiple convergence points exist
/// (nested diamonds), the strictest policy wins (`Slowest > Fastest`).
///
/// Only meaningful when `diamond_consistency = 'atomic'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiamondSchedulePolicy {
    /// Fire the group when **any** member is due (higher freshness).
    #[default]
    Fastest,
    /// Fire the group only when **all** members are due (lower resource cost).
    Slowest,
}

impl DiamondSchedulePolicy {
    /// Serialize to the SQL CHECK constraint value.
    pub fn as_str(&self) -> &'static str {
        match self {
            DiamondSchedulePolicy::Fastest => "fastest",
            DiamondSchedulePolicy::Slowest => "slowest",
        }
    }

    /// Deserialize from SQL string. Returns `None` for unrecognized values.
    pub fn from_sql_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "fastest" => Some(Self::Fastest),
            "slowest" => Some(Self::Slowest),
            _ => None,
        }
    }

    /// Return the stricter of two policies (`Slowest > Fastest`).
    pub fn stricter(self, other: Self) -> Self {
        if self == Self::Slowest || other == Self::Slowest {
            Self::Slowest
        } else {
            Self::Fastest
        }
    }
}

impl std::fmt::Display for DiamondSchedulePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A detected diamond in the ST dependency graph.
///
/// A diamond exists when two or more paths from shared source(s) converge at a
/// single fan-in stream table via different intermediate STs.
///
/// Example: `A → B → D` and `A → C → D` — convergence is D, shared source is
/// A, intermediates are {B, C}.
#[derive(Debug, Clone)]
pub struct Diamond {
    /// The fan-in ST that joins two or more upstream paths.
    pub convergence: NodeId,
    /// Base tables / STs that are the shared root(s) of the converging paths.
    pub shared_sources: Vec<NodeId>,
    /// Intermediate STs on all paths from shared_sources to convergence
    /// (excludes both the convergence node and the shared sources).
    pub intermediates: Vec<NodeId>,
}

/// A set of stream tables that must refresh atomically to maintain cross-path
/// consistency.
///
/// When `diamond_consistency = 'atomic'`, all members are wrapped in a single
/// SAVEPOINT. If any member's refresh fails, the entire group is rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

#[derive(Debug, Clone)]
pub struct ConsistencyGroup {
    /// All members in topological order, including the convergence ST (last).
    pub members: Vec<NodeId>,
    /// The fan-in ST(s) that required this group.
    pub convergence_points: Vec<NodeId>,
    /// Monotonically increasing counter; advances on every successful group
    /// refresh.
    pub epoch: u64,
    /// The transactional isolation level for this group.
    pub isolation_level: IsolationLevel,
}

impl ConsistencyGroup {
    /// Returns `true` when the group contains only a single ST.
    ///
    /// Singleton groups represent non-diamond STs and skip the SAVEPOINT
    /// overhead in the scheduler.
    pub fn is_singleton(&self) -> bool {
        self.members.len() == 1
    }

    /// Advance the epoch counter after a successful group refresh.
    pub fn advance_epoch(&mut self) {
        self.epoch += 1;
    }
}

#[cfg(feature = "pg18")]
use pgrx::prelude::*;

// ── Strongly Connected Components ──────────────────────────────────────────

/// A strongly connected component of the dependency graph.
///
/// Used by the circular dependency foundation (CYC-1) to group nodes that
/// form mutual dependencies (cycles). Singleton SCCs represent acyclic
/// nodes; multi-node SCCs contain cycles that require fixed-point iteration.
#[derive(Debug, Clone)]
pub struct Scc {
    /// Node IDs in this SCC.
    pub nodes: Vec<NodeId>,
    /// True if this SCC contains a cycle (either a multi-node SCC or a
    /// self-loop).
    pub is_cyclic: bool,
}

/// Identifies a node in the dependency graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeId {
    /// A regular base table or view, identified by PostgreSQL OID.
    BaseTable(u32),
    /// A stream table, identified by its `pgt_id` from the catalog.
    StreamTable(i64),
}

/// Status of a stream table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StStatus {
    Initializing,
    Active,
    Suspended,
    Error,
}

impl StStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            StStatus::Initializing => "INITIALIZING",
            StStatus::Active => "ACTIVE",
            StStatus::Suspended => "SUSPENDED",
            StStatus::Error => "ERROR",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, PgTrickleError> {
        match s {
            "INITIALIZING" => Ok(StStatus::Initializing),
            "ACTIVE" => Ok(StStatus::Active),
            "SUSPENDED" => Ok(StStatus::Suspended),
            "ERROR" => Ok(StStatus::Error),
            other => Err(PgTrickleError::InvalidArgument(format!(
                "unknown status: {other}"
            ))),
        }
    }
}

/// Refresh mode for a stream table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    Full,
    Differential,
    /// Immediate (transactional) IVM — stream table is updated within the same
    /// transaction as the base table DML, using statement-level AFTER triggers
    /// with transition tables.
    Immediate,
}

impl RefreshMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefreshMode::Full => "FULL",
            RefreshMode::Differential => "DIFFERENTIAL",
            RefreshMode::Immediate => "IMMEDIATE",
        }
    }

    /// Returns true if this mode uses a background schedule for refresh.
    pub fn is_scheduled(&self) -> bool {
        matches!(self, RefreshMode::Full | RefreshMode::Differential)
    }

    /// Returns true if this is IMMEDIATE (transactional) mode.
    pub fn is_immediate(&self) -> bool {
        matches!(self, RefreshMode::Immediate)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, PgTrickleError> {
        match s.to_uppercase().as_str() {
            "FULL" => Ok(RefreshMode::Full),
            "DIFFERENTIAL" => Ok(RefreshMode::Differential),
            "IMMEDIATE" => Ok(RefreshMode::Immediate),
            // Accept INCREMENTAL as a deprecated alias for backward compatibility.
            "INCREMENTAL" => Ok(RefreshMode::Differential),
            // AUTO is handled at the API layer (create_stream_table_impl);
            // it should never reach from_str because the caller resolves it
            // before calling this function.
            "AUTO" => Ok(RefreshMode::Differential),
            other => Err(PgTrickleError::InvalidArgument(format!(
                "unknown refresh mode: {other}. Must be 'AUTO', 'FULL', 'DIFFERENTIAL', or 'IMMEDIATE'"
            ))),
        }
    }

    /// Returns true if the given string represents the AUTO sentinel value.
    pub fn is_auto_str(s: &str) -> bool {
        s.eq_ignore_ascii_case("AUTO")
    }
}

/// Metadata for a node in the DAG.
#[derive(Debug, Clone)]
pub struct DagNode {
    pub id: NodeId,
    /// User-specified schedule. `None` means CALCULATED or cron-scheduled.
    pub schedule: Option<Duration>,
    /// Resolved effective schedule (including CALCULATED resolution).
    pub effective_schedule: Duration,
    /// Name for display and error messages.
    pub name: String,
    /// Status of this ST (only meaningful for ST nodes).
    pub status: StStatus,
    /// Raw schedule string from the catalog (e.g. "5m" or "*/5 * * * *").
    /// `None` for CALCULATED.
    pub schedule_raw: Option<String>,
}

/// In-memory dependency graph of stream tables and their sources.
#[derive(Clone)]
pub struct StDag {
    /// Forward edges: source → list of downstream ST node IDs.
    edges: HashMap<NodeId, Vec<NodeId>>,
    /// Reverse edges: ST node → list of upstream source node IDs.
    reverse_edges: HashMap<NodeId, Vec<NodeId>>,
    /// Node metadata (only for ST nodes).
    nodes: HashMap<NodeId, DagNode>,
    /// All node IDs in the graph.
    all_nodes: HashSet<NodeId>,
    /// C2-2: Cached topological sort result (all nodes incl. base tables).
    /// Populated lazily on first `topological_sort_inner()` call and
    /// invalidated when edges or nodes change.
    cached_topo: std::cell::RefCell<Option<Vec<NodeId>>>,
    /// A46-9: Monotone generation counter. Incremented every time `resolve_calculated_schedule`
    /// runs to completion so callers can detect stale cached schedules.
    pub schedule_generation: u64,
}

impl StDag {
    /// Create an empty DAG.
    pub fn new() -> Self {
        StDag {
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            nodes: HashMap::new(),
            all_nodes: HashSet::new(),
            cached_topo: std::cell::RefCell::new(None),
            schedule_generation: 0,
        }
    }

    /// Build the DAG from the catalog tables via SPI.
    ///
    /// Loads all stream tables and their dependencies, constructs the graph,
    /// and resolves CALCULATED schedules.
    #[cfg(feature = "pg18")]
    pub fn build_from_catalog(fallback_schedule_secs: i32) -> Result<Self, PgTrickleError> {
        let mut dag = StDag::new();

        Spi::connect(|client| {
            // Load all stream tables
            let st_table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, \
                     schedule AS schedule_secs, \
                     status, refresh_mode, is_populated, needs_reinit \
                     FROM pgtrickle.pgt_stream_tables",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            for row in st_table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

                let pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                let _pgt_relid = row.get::<pg_sys::Oid>(2).map_err(map_spi)?;
                let pgt_name = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
                let pgt_schema = row.get::<String>(4).map_err(map_spi)?.unwrap_or_default();
                let schedule_text = row.get::<String>(5).map_err(map_spi)?;
                let status_str = row.get::<String>(6).map_err(map_spi)?.unwrap_or_default();
                let _mode_str = row.get::<String>(7).map_err(map_spi)?.unwrap_or_default();

                // For duration-based schedule, parse to Duration.
                // For cron expressions, treat as None (CALCULATED) for DAG
                // resolution — cron STs are scheduled independently.
                let schedule = schedule_text.as_ref().and_then(|s| {
                    crate::api::parse_duration(s)
                        .ok()
                        .map(|secs| Duration::from_secs(secs.max(0) as u64))
                });
                let status = StStatus::from_str(&status_str).unwrap_or(StStatus::Error);
                let effective_schedule = schedule.unwrap_or(Duration::ZERO);

                dag.add_st_node(DagNode {
                    id: NodeId::StreamTable(pgt_id),
                    schedule,
                    effective_schedule,
                    name: format!("{}.{}", pgt_schema, pgt_name),
                    status,
                    schedule_raw: schedule_text,
                });
            }

            // Load all dependency edges
            let dep_table = client
                .select(
                    "SELECT d.pgt_id, d.source_relid, d.source_type, \
                     st.pgt_id AS source_pgt_id \
                     FROM pgtrickle.pgt_dependencies d \
                     LEFT JOIN pgtrickle.pgt_stream_tables st ON st.pgt_relid = d.source_relid",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            for row in dep_table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

                let pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                let source_relid = row
                    .get::<pg_sys::Oid>(2)
                    .map_err(map_spi)?
                    .unwrap_or(pg_sys::InvalidOid);
                let source_pgt_id = row.get::<i64>(4).map_err(map_spi)?;

                // Determine the source node type
                let source_node = match source_pgt_id {
                    Some(src_pgt_id) => NodeId::StreamTable(src_pgt_id),
                    None => NodeId::BaseTable(source_relid.to_u32()),
                };

                let downstream_node = NodeId::StreamTable(pgt_id);
                dag.add_edge(source_node, downstream_node);
            }

            Ok::<(), PgTrickleError>(())
        })?;

        // Resolve CALCULATED schedules
        dag.resolve_calculated_schedule(fallback_schedule_secs);

        Ok(dag)
    }

    /// C2-2: Invalidate the cached topological sort.
    ///
    /// Must be called after any structural change to the graph.
    fn invalidate_topo_cache(&self) {
        *self.cached_topo.borrow_mut() = None;
    }

    /// Add a stream table node to the DAG.
    pub fn add_st_node(&mut self, node: DagNode) {
        let id = node.id;
        self.all_nodes.insert(id);
        self.nodes.insert(id, node);
        self.invalidate_topo_cache();
    }

    /// Add an edge from `source` to `downstream_st`.
    pub fn add_edge(&mut self, source: NodeId, downstream_st: NodeId) {
        self.all_nodes.insert(source);
        self.all_nodes.insert(downstream_st);
        self.edges.entry(source).or_default().push(downstream_st);
        self.reverse_edges
            .entry(downstream_st)
            .or_default()
            .push(source);
        self.invalidate_topo_cache();
    }

    /// Remove all incoming edges for a node and replace them with new ones.
    ///
    /// Used by ALTER QUERY cycle detection: the existing ST keeps its node
    /// but its source edges are replaced with the proposed new sources.
    pub fn replace_incoming_edges(&mut self, target: NodeId, new_sources: Vec<NodeId>) {
        // Remove old incoming edges
        if let Some(old_sources) = self.reverse_edges.remove(&target) {
            for src in &old_sources {
                if let Some(dsts) = self.edges.get_mut(src) {
                    dsts.retain(|d| *d != target);
                }
            }
        }
        self.invalidate_topo_cache();
        // Add new incoming edges (each add_edge call also invalidates cache)
        for src in new_sources {
            self.add_edge(src, target);
        }
    }

    /// Get all upstream sources of a stream table node.
    pub fn get_upstream(&self, node: NodeId) -> Vec<NodeId> {
        self.reverse_edges.get(&node).cloned().unwrap_or_default()
    }

    /// Get all immediate downstream dependents of a node.
    pub fn get_downstream(&self, node: NodeId) -> Vec<NodeId> {
        self.edges.get(&node).cloned().unwrap_or_default()
    }

    /// Get all ST nodes in the graph.
    pub fn get_all_st_nodes(&self) -> Vec<&DagNode> {
        self.nodes.values().collect()
    }

    /// Get a stream table node by its `NodeId`, if it exists.
    pub fn get_node(&self, id: &NodeId) -> Option<&DagNode> {
        self.nodes.get(id)
    }

    /// Human-readable label for any node (ST or base table).
    pub fn node_label(&self, node: &NodeId) -> String {
        match self.nodes.get(node) {
            Some(n) => n.name.clone(),
            None => match node {
                NodeId::BaseTable(oid) => format!("base_table(oid={})", oid),
                NodeId::StreamTable(id) => format!("stream_table(id={})", id),
            },
        }
    }

    /// Returns `true` if the given pgt_id is present in the DAG.
    ///
    /// Used by the scheduler to verify that all invalidated pgt_ids are
    /// visible after a DAG rebuild.  If any are missing (stale catalog
    /// snapshot), the scheduler should not update its `dag_version` so the
    /// next tick rebuilds with a fresh snapshot.
    pub fn has_st_node(&self, pgt_id: i64) -> bool {
        self.all_nodes.contains(&NodeId::StreamTable(pgt_id))
    }

    /// Detect cycles using Kahn's algorithm (BFS topological sort).
    ///
    /// Returns `Ok(())` if the graph is acyclic, or `Err(CycleDetected)` with
    /// the names of nodes involved in the cycle.
    pub fn detect_cycles(&self) -> Result<(), PgTrickleError> {
        let topo = self.topological_sort_inner()?;
        if topo.len() < self.all_nodes.len() {
            // Some nodes were not processed → cycle exists.
            let processed: HashSet<_> = topo.into_iter().collect();
            let cycle_nodes: Vec<String> = self
                .all_nodes
                .iter()
                .filter(|n| !processed.contains(n))
                .map(|n| self.node_name(n))
                .collect();
            // OPS-10-02: Count cycle detections for Prometheus alerting.
            crate::shmem::increment_dag_cycles_detected();
            Err(PgTrickleError::CycleDetected(cycle_nodes))
        } else {
            Ok(())
        }
    }

    /// Return ST nodes in topological order (upstream first).
    ///
    /// Only returns `NodeId::StreamTable` entries; base tables are excluded
    /// from the output since they don't need refreshing.
    pub fn topological_order(&self) -> Result<Vec<NodeId>, PgTrickleError> {
        self.detect_cycles()?;
        let all = self.topological_sort_inner()?;
        Ok(all
            .into_iter()
            .filter(|n| matches!(n, NodeId::StreamTable(_)))
            .collect())
    }

    /// Return ST nodes grouped by parallelism level (upstream first).
    ///
    /// Level 0 contains all zero-indegree nodes (no upstream ST dependencies),
    /// level 1 contains nodes whose dependencies are all in level 0, etc.
    /// Nodes within the same level have no dependency edges between them and
    /// can be refreshed in parallel.
    ///
    /// Only returns `NodeId::StreamTable` entries; base tables are excluded.
    pub fn topological_levels(&self) -> Result<Vec<Vec<NodeId>>, PgTrickleError> {
        // Compute in-degrees (same start as Kahn's algorithm).
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
        for &node in &self.all_nodes {
            in_degree.entry(node).or_insert(0);
        }
        for targets in self.edges.values() {
            for &target in targets {
                *in_degree.entry(target).or_insert(0) += 1;
            }
        }

        let mut levels: Vec<Vec<NodeId>> = Vec::new();
        let mut processed = 0usize;

        // Seed: all zero-indegree nodes form level 0.
        let mut current_level: Vec<NodeId> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&node, _)| node)
            .collect();

        while !current_level.is_empty() {
            // Filter to ST nodes for the output level.
            let st_level: Vec<NodeId> = current_level
                .iter()
                .filter(|n| matches!(n, NodeId::StreamTable(_)))
                .copied()
                .collect();
            if !st_level.is_empty() {
                levels.push(st_level);
            }

            processed += current_level.len();

            // Compute next level: decrement in-degrees for downstream nodes.
            let mut next_level = Vec::new();
            for node in &current_level {
                if let Some(downstream) = self.edges.get(node) {
                    for &d in downstream {
                        if let Some(deg) = in_degree.get_mut(&d) {
                            *deg -= 1;
                            if *deg == 0 {
                                next_level.push(d);
                            }
                        }
                    }
                }
            }
            current_level = next_level;
        }

        if processed < self.all_nodes.len() {
            let cycle_nodes: Vec<String> = self
                .all_nodes
                .iter()
                .filter(|n| in_degree.get(n).is_some_and(|&d| d > 0))
                .map(|n| self.node_name(n))
                .collect();
            return Err(PgTrickleError::CycleDetected(cycle_nodes));
        }

        Ok(levels)
    }

    /// Resolve CALCULATED schedules.
    ///
    /// For STs with `schedule = None` (CALCULATED), compute the effective schedule
    /// as `MIN(schedule)` across all immediate downstream dependents.
    /// If no downstream dependents exist, use a fallback (the min schedule GUC).
    pub fn resolve_calculated_schedule(&mut self, fallback_seconds: i32) {
        let fallback = Duration::from_secs(fallback_seconds as u64);

        // Iterate until convergence (at most |V| iterations).
        let mut changed = true;
        let mut iterations = 0;
        let max_iterations = self.nodes.len() + 1;

        while changed && iterations < max_iterations {
            changed = false;
            iterations += 1;

            let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for id in node_ids {
                let node = &self.nodes[&id];
                if let Some(tl) = node.schedule {
                    // Explicit schedule — effective_schedule = schedule.
                    if self.nodes[&id].effective_schedule != tl
                        && let Some(node) = self.nodes.get_mut(&id)
                    {
                        node.effective_schedule = tl;
                        changed = true;
                    }
                    continue;
                }

                // CALCULATED: MIN(effective_schedule) of immediate downstream STs.
                let downstream = self.get_downstream(id);
                let min_schedule = downstream
                    .iter()
                    .filter_map(|d| self.nodes.get(d))
                    .map(|d| d.effective_schedule)
                    .min()
                    .unwrap_or(fallback);

                if self.nodes[&id].effective_schedule != min_schedule
                    && let Some(node) = self.nodes.get_mut(&id)
                {
                    node.effective_schedule = min_schedule;
                    changed = true;
                }
            }
        }

        // A46-9: Advance generation so callers can detect stale cached results.
        self.schedule_generation = self.schedule_generation.wrapping_add(1);
    }

    /// A46-9: Re-resolve CALCULATED schedules for a targeted subset of nodes
    /// and their upstream transitive closure.
    ///
    /// For incremental DAG rebuilds touching a small set of nodes, this avoids
    /// the O(V) full resolution pass. Instead, it collects all upstream
    /// CALCULATED ancestors of the affected set and only re-resolves those,
    /// propagating bottom-up until convergence.
    ///
    /// Falls back to `resolve_calculated_schedule` for safety when:
    /// - Any affected node is CALCULATED (removing it may change all upstream paths)
    /// - The affected set is large relative to the DAG size (heuristic: >25%)
    pub fn resolve_calculated_schedule_incremental(
        &mut self,
        affected_ids: &[i64],
        fallback_seconds: i32,
    ) {
        let dag_size = self.nodes.len();
        if dag_size == 0 {
            return;
        }

        // Heuristic: if the change touches more than 25% of nodes, full rebuild
        // is not much more expensive and is guaranteed correct.
        if affected_ids.len() * 4 >= dag_size {
            self.resolve_calculated_schedule(fallback_seconds);
            return;
        }

        let fallback = Duration::from_secs(fallback_seconds as u64);

        // Collect upstream CALCULATED ancestors (BFS from affected nodes going upstream).
        let mut to_resolve: Vec<NodeId> = Vec::new();
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut queue: Vec<NodeId> = affected_ids
            .iter()
            .map(|&id| NodeId::StreamTable(id))
            .collect();

        while let Some(node_id) = queue.pop() {
            if !visited.insert(node_id) {
                continue;
            }
            // Only resolve ST nodes with CALCULATED schedules (schedule == None)
            if self
                .nodes
                .get(&node_id)
                .is_some_and(|n| n.schedule.is_none())
            {
                to_resolve.push(node_id);
            }
            // Walk upstream to find CALCULATED ancestors
            for up in self.get_upstream(node_id) {
                if !visited.contains(&up) {
                    queue.push(up);
                }
            }
        }

        if to_resolve.is_empty() {
            self.schedule_generation = self.schedule_generation.wrapping_add(1);
            return;
        }

        // Re-resolve only the collected CALCULATED nodes (convergence pass).
        let max_iter = to_resolve.len() + 1;
        let mut changed = true;
        let mut iterations = 0;
        while changed && iterations < max_iter {
            changed = false;
            iterations += 1;
            for &id in &to_resolve {
                let downstream = self.get_downstream(id);
                let min_schedule = downstream
                    .iter()
                    .filter_map(|d| self.nodes.get(d))
                    .map(|d| d.effective_schedule)
                    .min()
                    .unwrap_or(fallback);
                if let Some(node) = self.nodes.get_mut(&id) {
                    if node.effective_schedule == min_schedule {
                        continue;
                    }
                    node.effective_schedule = min_schedule;
                    changed = true;
                }
            }
        }

        self.schedule_generation = self.schedule_generation.wrapping_add(1);
    }

    // ── Diamond dependency detection ──────────────────────────────────

    /// S-1 (v0.54.0): Precompute transitive ancestor sets for all nodes in
    /// a single O(V+E) topological traversal.
    ///
    /// Processes nodes in **forward** topological order (sources/roots first)
    /// so that when a node V is processed, all its parents' ancestor sets are
    /// already complete.  Each node's ancestor set is built by unioning the
    /// parent ancestor sets plus the parent itself.
    ///
    /// `ancestors_of(V) = ∪ ({P} ∪ ancestors_of(P))` for each parent P of V.
    ///
    /// This replaces the per-branch `collect_ancestors()` calls in
    /// `detect_diamonds()`, which were O(V) per fan-in branch and O(V²) total.
    /// With memoized ancestor sets, the total work for ancestor union operations
    /// is bounded by the sum of out-degrees, which is O(V+E).
    fn compute_all_ancestors(&self) -> HashMap<NodeId, HashSet<NodeId>> {
        let topo = self.topological_order().unwrap_or_default();
        let mut ancestor_sets: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

        // Process in FORWARD topological order (sources/roots first).
        // When processing node V, all parents' ancestor sets are already computed
        // because parents precede their children in topological order.
        for &node in topo.iter() {
            let mut ancestors: HashSet<NodeId> = HashSet::new();
            if let Some(upstream_nodes) = self.reverse_edges.get(&node) {
                for &up in upstream_nodes {
                    // Add the parent itself.
                    ancestors.insert(up);
                    // Union in the parent's already-computed ancestor set.
                    if let Some(up_ancestors) = ancestor_sets.get(&up) {
                        ancestors.extend(up_ancestors.iter().copied());
                    }
                }
            }
            ancestor_sets.insert(node, ancestors);
        }

        ancestor_sets
    }

    /// Detect all diamond dependencies in the DAG.
    ///
    /// A diamond exists when a fan-in ST node D (with ≥2 upstream ST
    /// dependencies) has two upstream paths that share a common ancestor.
    ///
    /// Returns a list of [`Diamond`] values with overlapping diamonds merged.
    ///
    /// S-1 (v0.54.0): Uses precomputed ancestor sets from `compute_all_ancestors()`
    /// (O(V+E) single pass) instead of per-branch `collect_ancestors()` calls
    /// (O(V²) pairwise).  For 500 stream tables, this reduces diamond detection
    /// from ~250ms to ~1ms.
    pub fn detect_diamonds(&self) -> Vec<Diamond> {
        let mut diamonds = Vec::new();

        // S-1: Precompute ALL ancestor sets in one O(V+E) pass.
        let ancestor_sets = self.compute_all_ancestors();

        // Find all fan-in nodes: STs with ≥2 upstream ST-or-base dependencies
        // that have at least 2 upstream *ST* dependencies (the interesting case
        // for atomic groups) or 2+ paths sharing a common ancestor via base
        // tables.
        for &node in &self.all_nodes {
            if !matches!(node, NodeId::StreamTable(_)) {
                continue;
            }
            let upstream = self.get_upstream(node);
            if upstream.len() < 2 {
                continue;
            }

            // S-1: Build branch ancestor sets from the precomputed map instead of
            // calling collect_ancestors() per branch. Each branch's ancestor set
            // is: ancestors_of(upstream_node) ∪ {upstream_node}.
            let branch_ancestors: Vec<(NodeId, HashSet<NodeId>)> = upstream
                .iter()
                .map(|&up| {
                    let mut anc_set = ancestor_sets.get(&up).cloned().unwrap_or_default();
                    anc_set.insert(up); // include the immediate upstream itself
                    (up, anc_set)
                })
                .collect();

            // Compare every pair of branches for shared ancestors.
            for i in 0..branch_ancestors.len() {
                for j in (i + 1)..branch_ancestors.len() {
                    // PERF-7: Use a lazy intersection iterator and break on
                    // the first shared node (common case: no shared nodes).
                    let has_shared = branch_ancestors[i]
                        .1
                        .intersection(&branch_ancestors[j].1)
                        .next()
                        .is_some();

                    if !has_shared {
                        continue;
                    }

                    let shared: Vec<NodeId> = branch_ancestors[i]
                        .1
                        .intersection(&branch_ancestors[j].1)
                        .copied()
                        .collect();

                    // Intermediates = union of both branches minus
                    // the convergence node and the shared sources.
                    let shared_set: HashSet<NodeId> = shared.iter().copied().collect();
                    let mut intermediates_set: HashSet<NodeId> = HashSet::new();

                    // Add STs from branch i between shared sources and convergence
                    for &anc in &branch_ancestors[i].1 {
                        if !shared_set.contains(&anc) && anc != node {
                            intermediates_set.insert(anc);
                        }
                    }
                    // Add STs from branch j between shared sources and convergence
                    for &anc in &branch_ancestors[j].1 {
                        if !shared_set.contains(&anc) && anc != node {
                            intermediates_set.insert(anc);
                        }
                    }

                    // Also include the immediate upstream nodes if they are STs
                    // and not themselves shared sources.
                    let up_i = branch_ancestors[i].0;
                    let up_j = branch_ancestors[j].0;
                    if !shared_set.contains(&up_i) {
                        intermediates_set.insert(up_i);
                    }
                    if !shared_set.contains(&up_j) {
                        intermediates_set.insert(up_j);
                    }

                    // Only keep ST nodes as intermediates (base tables don't refresh).
                    let intermediates: Vec<NodeId> = intermediates_set
                        .into_iter()
                        .filter(|n| matches!(n, NodeId::StreamTable(_)))
                        .collect();

                    diamonds.push(Diamond {
                        convergence: node,
                        shared_sources: shared,
                        intermediates,
                    });
                }
            }
        }

        Self::merge_overlapping_diamonds(diamonds)
    }

    /// Compute consistency groups for the scheduler.
    ///
    /// Each detected diamond produces a group containing the intermediate STs
    /// and the convergence ST, in topological order. Overlapping groups are
    /// merged transitively. STs not in any diamond get singleton groups.
    pub fn compute_consistency_groups(&self) -> Vec<ConsistencyGroup> {
        let diamonds = self.detect_diamonds();

        // Build a mapping: NodeId → set index, to enable union-find merging.
        let mut node_to_group: HashMap<NodeId, usize> = HashMap::new();
        let mut groups: Vec<HashSet<NodeId>> = Vec::new();
        let mut convergence_map: Vec<HashSet<NodeId>> = Vec::new();

        for diamond in &diamonds {
            // Members = intermediates ∪ {convergence}
            let mut members: HashSet<NodeId> = diamond.intermediates.iter().copied().collect();
            members.insert(diamond.convergence);

            // Check if any existing group overlaps with this one.
            let overlapping: Vec<usize> = members
                .iter()
                .filter_map(|n| node_to_group.get(n).copied())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            if overlapping.is_empty() {
                // New group.
                let idx = groups.len();
                for &m in &members {
                    node_to_group.insert(m, idx);
                }
                groups.push(members);
                let mut cp = HashSet::new();
                cp.insert(diamond.convergence);
                convergence_map.push(cp);
            } else {
                // Merge into the first overlapping group.
                let target = overlapping[0];
                for &m in &members {
                    node_to_group.insert(m, target);
                    groups[target].insert(m);
                }
                convergence_map[target].insert(diamond.convergence);

                // Merge other overlapping groups into target.
                for &other in overlapping.iter().skip(1) {
                    let other_members: HashSet<NodeId> = groups[other].drain().collect();
                    for m in &other_members {
                        node_to_group.insert(*m, target);
                    }
                    groups[target].extend(other_members);
                    let other_cp: HashSet<NodeId> = convergence_map[other].drain().collect();
                    convergence_map[target].extend(other_cp);
                }
            }
        }

        // Sort each group's members in topological order.
        let topo_order = self.topological_order().unwrap_or_default();
        let topo_pos: HashMap<NodeId, usize> = topo_order
            .iter()
            .enumerate()
            .map(|(i, &n)| (n, i))
            .collect();

        let mut result: Vec<ConsistencyGroup> = Vec::new();
        let mut assigned: HashSet<NodeId> = HashSet::new();

        for (i, group_set) in groups.iter().enumerate() {
            if group_set.is_empty() {
                continue; // Merged away.
            }
            let mut members: Vec<NodeId> = group_set.iter().copied().collect();
            members.sort_by_key(|n| topo_pos.get(n).copied().unwrap_or(usize::MAX));

            let convergence_points: Vec<NodeId> = convergence_map[i].iter().copied().collect();

            for &m in &members {
                assigned.insert(m);
            }

            result.push(ConsistencyGroup {
                members,
                convergence_points,
                epoch: 0,
                isolation_level: IsolationLevel::ReadCommitted,
            });
        }

        // Add singleton groups for all STs not in any diamond group.
        for &node in &self.all_nodes {
            if matches!(node, NodeId::StreamTable(_)) && !assigned.contains(&node) {
                result.push(ConsistencyGroup {
                    members: vec![node],
                    convergence_points: vec![],
                    epoch: 0,
                    isolation_level: IsolationLevel::ReadCommitted,
                });
            }
        }

        // Sort groups by the topological position of their first member
        // so the scheduler processes them in dependency order.
        result.sort_by_key(|g| {
            g.members
                .first()
                .and_then(|n| topo_pos.get(n).copied())
                .unwrap_or(usize::MAX)
        });

        result
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Kahn's algorithm: BFS topological sort.
    ///
    /// C2-2: Results are cached and reused until the graph structure changes.
    /// This avoids redundant O(V+E) recomputation within the same scheduler
    /// tick (called by both `detect_cycles` and `topological_order`).
    fn topological_sort_inner(&self) -> Result<Vec<NodeId>, PgTrickleError> {
        // C2-2: Return cached result if available.
        if let Some(cached) = self.cached_topo.borrow().as_ref() {
            return Ok(cached.clone());
        }

        // Compute in-degrees.
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
        for &node in &self.all_nodes {
            in_degree.entry(node).or_insert(0);
        }
        for targets in self.edges.values() {
            for &target in targets {
                *in_degree.entry(target).or_insert(0) += 1;
            }
        }

        // Enqueue zero-indegree nodes.
        let mut queue: VecDeque<NodeId> = in_degree
            .iter()
            .filter(|&(_, deg)| *deg == 0)
            .map(|(&node, _)| node)
            .collect();

        let mut result = Vec::with_capacity(self.all_nodes.len());

        while let Some(node) = queue.pop_front() {
            result.push(node);
            if let Some(downstream) = self.edges.get(&node) {
                for &d in downstream {
                    if let Some(deg) = in_degree.get_mut(&d) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(d);
                        }
                    }
                }
            }
        }

        // C2-2: Cache the result for subsequent calls in this tick.
        *self.cached_topo.borrow_mut() = Some(result.clone());

        Ok(result)
    }

    /// Human-readable name for a node.
    fn node_name(&self, node: &NodeId) -> String {
        self.node_label(node)
    }

    /// Recursively collect all transitive ancestors of `node` (upstream walk).
    fn collect_ancestors(&self, node: NodeId, ancestors: &mut HashSet<NodeId>) {
        if let Some(upstream) = self.reverse_edges.get(&node) {
            for &up in upstream {
                if ancestors.insert(up) {
                    self.collect_ancestors(up, ancestors);
                }
            }
        }
    }

    /// Merge diamonds whose `intermediates` sets overlap into a single diamond.
    ///
    /// This handles nested diamonds (e.g., D and G both being fan-in nodes
    /// sharing an intermediate ST) by transitively merging overlapping sets.
    fn merge_overlapping_diamonds(diamonds: Vec<Diamond>) -> Vec<Diamond> {
        if diamonds.is_empty() {
            return diamonds;
        }

        // Union-Find approach: group diamonds whose intermediates overlap.
        let mut merged: Vec<Diamond> = Vec::new();

        for diamond in diamonds {
            let int_set: HashSet<NodeId> = diamond.intermediates.iter().copied().collect();

            // Find which merged diamond overlaps with this one.
            let mut merge_target: Option<usize> = None;
            for (i, existing) in merged.iter().enumerate() {
                let existing_set: HashSet<NodeId> =
                    existing.intermediates.iter().copied().collect();
                if !int_set.is_disjoint(&existing_set) {
                    merge_target = Some(i);
                    break;
                }
            }

            match merge_target {
                Some(idx) => {
                    // Merge into existing diamond.
                    let existing = &mut merged[idx];
                    let mut combined_int: HashSet<NodeId> =
                        existing.intermediates.iter().copied().collect();
                    combined_int.extend(diamond.intermediates);
                    existing.intermediates = combined_int.into_iter().collect();

                    let mut combined_sources: HashSet<NodeId> =
                        existing.shared_sources.iter().copied().collect();
                    combined_sources.extend(diamond.shared_sources);
                    existing.shared_sources = combined_sources.into_iter().collect();

                    // If the convergence points differ, the original convergence
                    // becomes an intermediate (it's now an interior node of a
                    // larger merged diamond), unless they're the same.
                    if diamond.convergence != existing.convergence {
                        // Keep the downstream-most convergence; add the other
                        // to intermediates. For simplicity, keep the new one's
                        // convergence as well — the consistency group computation
                        // handles multiple convergence points.
                    }
                }
                None => {
                    merged.push(diamond);
                }
            }
        }

        merged
    }

    // ── Strongly Connected Components (CYC-1) ──────────────────────────

    /// Compute SCCs using Tarjan's algorithm (1972).
    ///
    /// Returns SCCs in **reverse topological order** of the condensation
    /// graph (i.e., upstream SCCs appear first). This is the natural output
    /// order of Tarjan's algorithm — SCCs are emitted in the order they are
    /// completed, which is reverse topological.
    ///
    /// Runs in O(V + E) time, same as Kahn's algorithm.
    ///
    /// # References
    ///
    /// - Tarjan, R.E. (1972). "Depth-first search and linear graph
    ///   algorithms." SIAM Journal on Computing, 1(2), 146–160.
    pub fn compute_sccs(&self) -> Result<Vec<Scc>, PgTrickleError> {
        let mut index_counter: u32 = 0;
        let mut stack: Vec<NodeId> = Vec::new();
        let mut on_stack: HashSet<NodeId> = HashSet::new();
        let mut indices: HashMap<NodeId, u32> = HashMap::new();
        let mut lowlinks: HashMap<NodeId, u32> = HashMap::new();
        let mut result: Vec<Scc> = Vec::new();

        for &node in &self.all_nodes {
            if !indices.contains_key(&node) {
                self.tarjan_strongconnect(
                    node,
                    &mut index_counter,
                    &mut stack,
                    &mut on_stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut result,
                )?;
            }
        }

        // Tarjan's emits SCCs in reverse topological order of the condensation
        // graph. Reverse so upstream SCCs come first.
        result.reverse();
        Ok(result)
    }

    /// Recursive DFS helper for Tarjan's algorithm.
    ///
    /// # Errors
    /// Returns `PgTrickleError::InternalError` if an SCC invariant is violated
    /// (unreachable in correct usage; guarded by `debug_assert!` in debug builds).
    #[allow(clippy::too_many_arguments)]
    fn tarjan_strongconnect(
        &self,
        v: NodeId,
        index_counter: &mut u32,
        stack: &mut Vec<NodeId>,
        on_stack: &mut HashSet<NodeId>,
        indices: &mut HashMap<NodeId, u32>,
        lowlinks: &mut HashMap<NodeId, u32>,
        result: &mut Vec<Scc>,
    ) -> Result<(), PgTrickleError> {
        // Set the depth index and lowlink for v.
        indices.insert(v, *index_counter);
        lowlinks.insert(v, *index_counter);
        *index_counter += 1;
        stack.push(v);
        on_stack.insert(v);

        // Consider successors of v.
        if let Some(successors) = self.edges.get(&v) {
            for &w in successors {
                if !indices.contains_key(&w) {
                    // Successor w has not yet been visited; recurse.
                    self.tarjan_strongconnect(
                        w,
                        index_counter,
                        stack,
                        on_stack,
                        indices,
                        lowlinks,
                        result,
                    )?;
                    let w_lowlink = lowlinks[&w];
                    debug_assert!(
                        lowlinks.contains_key(&v),
                        "SCC invariant: lowlinks must contain v after recursive call"
                    );
                    let v_lowlink = lowlinks.get_mut(&v).ok_or_else(|| {
                        PgTrickleError::InternalError(
                            "SCC invariant violated: lowlinks missing v after recursive call"
                                .to_string(),
                        )
                    })?;
                    if w_lowlink < *v_lowlink {
                        *v_lowlink = w_lowlink;
                    }
                } else if on_stack.contains(&w) {
                    // Successor w is on the stack → it's in the current SCC.
                    let w_index = indices[&w];
                    debug_assert!(
                        lowlinks.contains_key(&v),
                        "SCC invariant: lowlinks must contain v for on-stack check"
                    );
                    let v_lowlink = lowlinks.get_mut(&v).ok_or_else(|| {
                        PgTrickleError::InternalError(
                            "SCC invariant violated: lowlinks missing v for on-stack check"
                                .to_string(),
                        )
                    })?;
                    if w_index < *v_lowlink {
                        *v_lowlink = w_index;
                    }
                }
            }
        }

        // If v is a root node, pop the stack and generate an SCC.
        if lowlinks[&v] == indices[&v] {
            let mut scc_nodes = Vec::new();
            loop {
                debug_assert!(
                    !stack.is_empty(),
                    "SCC loop invariant: stack must be non-empty when root node found"
                );
                let w = stack.pop().ok_or_else(|| {
                    PgTrickleError::InternalError(
                        "SCC loop invariant violated: stack empty while popping SCC members"
                            .to_string(),
                    )
                })?;
                on_stack.remove(&w);
                scc_nodes.push(w);
                if w == v {
                    break;
                }
            }

            // An SCC is cyclic if it has more than one node, or if the
            // single node has a self-loop.
            let is_cyclic = if scc_nodes.len() > 1 {
                true
            } else {
                // Check for self-loop.
                let node = scc_nodes[0];
                self.edges
                    .get(&node)
                    .is_some_and(|succs| succs.contains(&node))
            };

            result.push(Scc {
                nodes: scc_nodes,
                is_cyclic,
            });
        }

        Ok(())
    }

    /// Return SCCs in refresh order (upstream first), with cyclic SCCs
    /// grouped for fixed-point iteration.
    ///
    /// Singleton SCCs (no cycle) are returned as single-node groups.
    /// Multi-node SCCs contain all members that must be iterated together.
    ///
    /// This is the condensation DAG in topological order — the replacement
    /// for `topological_order()` when circular dependencies are allowed.
    ///
    /// # Errors
    /// Returns `PgTrickleError::InternalError` if an SCC invariant is violated
    /// (unreachable in correct usage).
    pub fn condensation_order(&self) -> Result<Vec<Scc>, PgTrickleError> {
        self.compute_sccs()
    }
}

impl Default for StDag {
    fn default() -> Self {
        Self::new()
    }
}

// ── G-8: Incremental DAG rebuild ──────────────────────────────────────────

impl StDag {
    /// G-8: Remove a stream table node and all its edges from the DAG.
    ///
    /// Cleans up forward edges (from this node to downstream dependents),
    /// reverse edges (from upstream sources to this node), and removes the
    /// node from metadata and all_nodes. Base table source nodes that become
    /// orphaned are left in place (harmless, they don't participate in
    /// scheduling).
    pub fn remove_st_node(&mut self, pgt_id: i64) {
        let node = NodeId::StreamTable(pgt_id);

        // Remove forward edges: this node → downstream
        if let Some(downstream) = self.edges.remove(&node) {
            for d in &downstream {
                if let Some(rev) = self.reverse_edges.get_mut(d) {
                    rev.retain(|n| *n != node);
                }
            }
        }

        // Remove reverse edges: upstream → this node
        if let Some(upstream) = self.reverse_edges.remove(&node) {
            for u in &upstream {
                if let Some(fwd) = self.edges.get_mut(u) {
                    fwd.retain(|n| *n != node);
                }
            }
        }

        // Remove node metadata and graph membership
        self.nodes.remove(&node);
        self.all_nodes.remove(&node);
        self.invalidate_topo_cache();
    }

    /// G-8: Incrementally rebuild only the affected stream tables.
    ///
    /// Instead of rebuilding the entire DAG from catalog O(V+E), this method:
    /// 1. Removes old nodes/edges for each affected `pgt_id`
    /// 2. Re-queries catalog for those specific stream tables and their deps
    /// 3. Re-adds nodes and edges
    /// 4. Re-resolves CALCULATED schedules
    ///
    /// Returns `Ok(())` on success. The caller should fall back to a full
    /// `build_from_catalog` if this returns `Err`.
    #[cfg(feature = "pg18")]
    pub fn rebuild_incremental(
        &mut self,
        affected_ids: &[i64],
        fallback_schedule_secs: i32,
    ) -> Result<(), PgTrickleError> {
        // Phase 1: Remove old nodes and edges for affected IDs.
        for &pgt_id in affected_ids {
            self.remove_st_node(pgt_id);
        }

        // Phase 2: Re-query catalog for only the affected stream tables.
        Spi::connect(|client| {
            for &pgt_id in affected_ids {
                // Re-load this stream table's metadata (if it still exists).
                let st_table = client
                    .select(
                        "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, \
                         schedule AS schedule_secs, \
                         status, refresh_mode, is_populated, needs_reinit \
                         FROM pgtrickle.pgt_stream_tables \
                         WHERE pgt_id = $1",
                        None,
                        &[pgt_id.into()],
                    )
                    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

                for row in st_table {
                    let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

                    let id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                    let _pgt_relid = row.get::<pg_sys::Oid>(2).map_err(map_spi)?;
                    let pgt_name = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
                    let pgt_schema = row.get::<String>(4).map_err(map_spi)?.unwrap_or_default();
                    let schedule_text = row.get::<String>(5).map_err(map_spi)?;
                    let status_str = row.get::<String>(6).map_err(map_spi)?.unwrap_or_default();

                    let schedule = schedule_text.as_ref().and_then(|s| {
                        crate::api::parse_duration(s)
                            .ok()
                            .map(|secs| Duration::from_secs(secs.max(0) as u64))
                    });
                    let status = StStatus::from_str(&status_str).unwrap_or(StStatus::Error);
                    let effective_schedule = schedule.unwrap_or(Duration::ZERO);

                    self.add_st_node(DagNode {
                        id: NodeId::StreamTable(id),
                        schedule,
                        effective_schedule,
                        name: format!("{}.{}", pgt_schema, pgt_name),
                        status,
                        schedule_raw: schedule_text,
                    });
                }
                // If the query returns no rows, the ST was dropped and we've
                // already removed it in Phase 1 — correct behavior.

                // Re-load dependencies for this pgt_id.
                let dep_table = client
                    .select(
                        "SELECT d.pgt_id, d.source_relid, d.source_type, \
                         st.pgt_id AS source_pgt_id \
                         FROM pgtrickle.pgt_dependencies d \
                         LEFT JOIN pgtrickle.pgt_stream_tables st \
                           ON st.pgt_relid = d.source_relid \
                         WHERE d.pgt_id = $1",
                        None,
                        &[pgt_id.into()],
                    )
                    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

                for row in dep_table {
                    let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

                    let dep_pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                    let source_relid = row
                        .get::<pg_sys::Oid>(2)
                        .map_err(map_spi)?
                        .unwrap_or(pg_sys::InvalidOid);
                    let source_pgt_id = row.get::<i64>(4).map_err(map_spi)?;

                    let source_node = match source_pgt_id {
                        Some(src_pgt_id) => NodeId::StreamTable(src_pgt_id),
                        None => NodeId::BaseTable(source_relid.to_u32()),
                    };

                    let downstream_node = NodeId::StreamTable(dep_pgt_id);
                    self.add_edge(source_node, downstream_node);
                }
            }

            Ok::<(), PgTrickleError>(())
        })?;

        // Phase 3: Re-resolve CALCULATED schedules.
        // A46-9: Use incremental resolution for the affected subset — avoids
        // O(V) iteration when only a few nodes changed.
        self.resolve_calculated_schedule_incremental(affected_ids, fallback_schedule_secs);

        Ok(())
    }
}

// ── Execution Unit DAG (Phase 0 + Phase 1 of parallel refresh) ────────────

/// Unique identifier for an execution unit in the parallel-refresh DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionUnitId(pub u64);

impl std::fmt::Display for ExecutionUnitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "eu_{}", self.0)
    }
}

/// Kind of execution unit — determines how the unit is executed by a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionUnitKind {
    /// A single stream table with no special grouping constraints.
    Singleton,
    /// An atomic consistency group — all members refreshed serially in one
    /// worker transaction with SAVEPOINT rollback on failure.
    AtomicGroup,
    /// An atomic consistency group requiring REPEATABLE READ isolation.
    RepeatableReadGroup,
    /// An IMMEDIATE-trigger closure — the root scheduled refresh fires
    /// synchronous downstream updates inside the same transaction.
    ImmediateClosure,
    /// A cyclic Strongly Connected Component — members are refreshed
    /// iteratively until fixpoint convergence is reached.
    CyclicScc,
    /// DAG-4: A chain of Singleton CALCULATED stream tables where each
    /// upstream has exactly one downstream consumer.  Members are refreshed
    /// sequentially in the same worker, passing deltas via temp tables
    /// instead of persistent change buffer tables.
    FusedChain,
}

impl ExecutionUnitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionUnitKind::Singleton => "singleton",
            ExecutionUnitKind::AtomicGroup => "atomic_group",
            ExecutionUnitKind::RepeatableReadGroup => "repeatable_read_group",
            ExecutionUnitKind::ImmediateClosure => "immediate_closure",
            ExecutionUnitKind::CyclicScc => "cyclic_scc",
            ExecutionUnitKind::FusedChain => "fused_chain",
        }
    }
}

impl std::fmt::Display for ExecutionUnitKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// One schedulable piece of work for a refresh worker.
///
/// An execution unit wraps one or more stream tables that must be refreshed
/// together (or individually for singletons) by a single worker.
#[derive(Debug, Clone)]
pub struct ExecutionUnit {
    /// Unique identifier for this unit.
    pub id: ExecutionUnitId,
    /// What kind of unit this is.
    pub kind: ExecutionUnitKind,
    /// Stream table IDs contained in this unit (pgt_ids).
    /// For singletons, this has exactly one element.
    pub member_pgt_ids: Vec<i64>,
    /// Primary stream table ID (for singleton-like units, the first element).
    pub root_pgt_id: i64,
    /// Human-readable label for logging.
    pub label: String,
}

impl ExecutionUnit {
    /// Compute a stable key for this unit that persists across DAG rebuilds.
    ///
    /// Used as the `unit_key` in `pgt_scheduler_jobs` to prevent duplicate
    /// in-flight jobs and correlate retry state.
    pub fn stable_key(&self) -> String {
        let prefix = match self.kind {
            ExecutionUnitKind::Singleton => "s",
            ExecutionUnitKind::AtomicGroup => "a",
            ExecutionUnitKind::ImmediateClosure => "i",
            ExecutionUnitKind::RepeatableReadGroup => "rg",
            ExecutionUnitKind::CyclicScc => "c",
            ExecutionUnitKind::FusedChain => "fc",
        };
        // member_pgt_ids are sorted during construction
        let ids: Vec<String> = self
            .member_pgt_ids
            .iter()
            .map(|id| id.to_string())
            .collect();
        format!("{}:{}", prefix, ids.join(","))
    }
}

/// Graph of execution units with dependency edges and ready-queue support.
///
/// Built from an `StDag` by:
/// 1. Computing consistency groups (diamond collapse).
/// 2. Detecting IMMEDIATE-trigger closures and collapsing them.
/// 3. Building unit-to-unit dependency edges.
/// 4. Computing topological order.
/// 5. DAG-4: Fusing single-consumer CALCULATED chains.
pub struct ExecutionUnitDag {
    /// All execution units, keyed by ID.
    units: HashMap<ExecutionUnitId, ExecutionUnit>,
    /// Forward edges: unit → list of downstream unit IDs.
    edges: HashMap<ExecutionUnitId, Vec<ExecutionUnitId>>,
    /// Reverse edges: unit → list of upstream unit IDs.
    reverse_edges: HashMap<ExecutionUnitId, Vec<ExecutionUnitId>>,
    /// pgt_id → ExecutionUnitId mapping for fast lookup.
    pgt_to_unit: HashMap<i64, ExecutionUnitId>,
}

/// DAG-4: Detect chains of Singleton CALCULATED units eligible for fusing.
///
/// A unit A can chain to B when:
/// 1. A is a Singleton
/// 2. A has exactly one downstream unit (B)
/// 3. B is a Singleton
/// 4. Both A and B use CALCULATED (Differential) refresh mode
///
/// Returns maximal chains (length ≥ 2) in topological order.
/// Each unit appears in at most one chain.
pub fn find_fusable_chains<F>(
    eu_dag: &ExecutionUnitDag,
    resolve_mode: &F,
) -> Vec<Vec<ExecutionUnitId>>
where
    F: Fn(i64) -> Option<RefreshMode>,
{
    let mut visited: HashSet<ExecutionUnitId> = HashSet::new();
    let mut chains: Vec<Vec<ExecutionUnitId>> = Vec::new();

    // Find chain heads: Singletons that are NOT the single-downstream of
    // another fusable Singleton (i.e., the start of a chain).
    // We iterate all units and try to extend forward greedily.
    // To avoid duplicates, skip units already claimed by a prior chain.
    let mut unit_ids: Vec<ExecutionUnitId> = eu_dag.units.keys().copied().collect();
    unit_ids.sort_by_key(|id| id.0); // deterministic ordering

    for uid in unit_ids {
        if visited.contains(&uid) {
            continue;
        }
        let unit = match eu_dag.units.get(&uid) {
            Some(u) => u,
            None => continue,
        };
        if unit.kind != ExecutionUnitKind::Singleton {
            continue;
        }
        if !is_calculated_unit(unit, resolve_mode) {
            continue;
        }

        // Try to build a chain starting from this unit.
        let mut chain = vec![uid];
        visited.insert(uid);

        let mut current = uid;
        loop {
            let downstream = eu_dag.edges.get(&current).cloned().unwrap_or_default();
            if downstream.len() != 1 {
                break; // Must have exactly one downstream
            }
            let next_id = downstream[0];
            if visited.contains(&next_id) {
                break;
            }
            let next_unit = match eu_dag.units.get(&next_id) {
                Some(u) => u,
                None => break,
            };
            if next_unit.kind != ExecutionUnitKind::Singleton {
                break;
            }
            if !is_calculated_unit(next_unit, resolve_mode) {
                break;
            }
            chain.push(next_id);
            visited.insert(next_id);
            current = next_id;
        }

        if chain.len() >= 2 {
            chains.push(chain);
        }
    }

    chains
}

/// Check whether all members of an execution unit use CALCULATED
/// (Differential) refresh mode.
fn is_calculated_unit<F>(unit: &ExecutionUnit, resolve_mode: &F) -> bool
where
    F: Fn(i64) -> Option<RefreshMode>,
{
    unit.member_pgt_ids
        .iter()
        .all(|&pgt_id| resolve_mode(pgt_id) == Some(RefreshMode::Differential))
}

impl ExecutionUnitDag {
    /// Build an execution unit DAG from an `StDag` and a function that resolves
    /// the refresh mode for each stream table pgt_id.
    ///
    /// The `resolve_mode` closure returns `Some(RefreshMode)` for known STs
    /// or `None` for unknown ones. In a real scheduler context, this calls into
    /// the catalog. In tests, it can be a simple HashMap lookup.
    pub fn build_from_st_dag<F>(st_dag: &StDag, resolve_mode: F) -> Self
    where
        F: Fn(i64) -> Option<RefreshMode>,
    {
        let mut eu_dag = ExecutionUnitDag {
            units: HashMap::new(),
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            pgt_to_unit: HashMap::new(),
        };

        let mut next_id: u64 = 1;
        let mut alloc_id = || {
            let id = ExecutionUnitId(next_id);
            next_id += 1;
            id
        };

        let mut assigned_pgt_ids: HashSet<i64> = HashSet::new();

        // Step 0: Identify Cyclic SCCs and eagerly build execution units for them.
        for scc in st_dag.condensation_order().unwrap_or_else(|e| {
            pgrx::warning!(
                "pg_trickle: SCC computation error in build_from_st_dag (returning empty SCC list): {}",
                e
            );
            Vec::new()
        }) {
            if scc.is_cyclic {
                let mut pgt_ids: Vec<i64> = scc
                    .nodes
                    .iter()
                    .filter_map(|n| {
                        if let NodeId::StreamTable(id) = n {
                            Some(*id)
                        } else {
                            None
                        }
                    })
                    .collect();

                if pgt_ids.is_empty() {
                    continue;
                }

                pgt_ids.sort();
                pgt_ids.dedup();

                let id = alloc_id();
                let root = pgt_ids[0];
                let label = format_unit_members(st_dag, &pgt_ids);

                let unit = ExecutionUnit {
                    id,
                    kind: ExecutionUnitKind::CyclicScc,
                    member_pgt_ids: pgt_ids.clone(),
                    root_pgt_id: root,
                    label,
                };

                eu_dag.units.insert(id, unit);
                for &pgt_id in &pgt_ids {
                    eu_dag.pgt_to_unit.insert(pgt_id, id);
                    assigned_pgt_ids.insert(pgt_id);
                }
            }
        }

        // Step 1: Compute consistency groups from the StDag.
        let groups = st_dag.compute_consistency_groups();

        // Step 2: Detect IMMEDIATE-mode stream tables for closure collapsing.
        // Collect all ST pgt_ids that are in IMMEDIATE mode.
        let immediate_pgt_ids: HashSet<i64> = st_dag
            .get_all_st_nodes()
            .iter()
            .filter_map(|node| {
                if let NodeId::StreamTable(id) = node.id
                    && resolve_mode(id) == Some(RefreshMode::Immediate)
                {
                    return Some(id);
                }
                None
            })
            .collect();

        // Step 3: Build execution units from groups, collapsing IMMEDIATE
        // closures conservatively.
        //
        // Strategy: For each consistency group, check if any member triggers
        // IMMEDIATE downstream propagation. If so, detect the full IMMEDIATE
        // closure and merge into a single unit.

        for group in &groups {
            let pgt_ids: Vec<i64> = group
                .members
                .iter()
                .filter_map(|m| match m {
                    NodeId::StreamTable(id) => Some(*id),
                    _ => None,
                })
                .collect();

            // Skip already-assigned (merged into an IMMEDIATE closure).
            let pgt_ids: Vec<i64> = pgt_ids
                .into_iter()
                .filter(|id| !assigned_pgt_ids.contains(id))
                .collect();
            if pgt_ids.is_empty() {
                continue;
            }

            // Check if this group has any IMMEDIATE downstream that should
            // be collapsed into the same unit.
            let mut closure_ids: Vec<i64> = Vec::new();
            let mut has_immediate_closure = false;

            for &pgt_id in &pgt_ids {
                let node = NodeId::StreamTable(pgt_id);
                // Check downstream nodes for IMMEDIATE mode
                for downstream in st_dag.get_downstream(node) {
                    if let NodeId::StreamTable(ds_id) = downstream
                        && immediate_pgt_ids.contains(&ds_id)
                        && !assigned_pgt_ids.contains(&ds_id)
                    {
                        has_immediate_closure = true;
                        // Recursively collect the full closure
                        collect_immediate_closure(
                            st_dag,
                            &immediate_pgt_ids,
                            &assigned_pgt_ids,
                            ds_id,
                            &mut closure_ids,
                        );
                    }
                }
            }

            if has_immediate_closure {
                // Merge the group members + IMMEDIATE closure into one unit.
                let mut all_ids: Vec<i64> = pgt_ids.clone();
                all_ids.extend(closure_ids);
                all_ids.sort();
                all_ids.dedup();

                let id = alloc_id();
                let root = all_ids[0];
                let label = if all_ids.len() == 1 {
                    format_unit_label(st_dag, all_ids[0])
                } else {
                    format!(
                        "immediate_closure({})",
                        format_unit_members(st_dag, &all_ids)
                    )
                };

                for &pid in &all_ids {
                    assigned_pgt_ids.insert(pid);
                    eu_dag.pgt_to_unit.insert(pid, id);
                }

                eu_dag.units.insert(
                    id,
                    ExecutionUnit {
                        id,
                        kind: ExecutionUnitKind::ImmediateClosure,
                        member_pgt_ids: all_ids,
                        root_pgt_id: root,
                        label,
                    },
                );
            } else if pgt_ids.len() == 1 {
                // Singleton unit.
                let pgt_id = pgt_ids[0];
                let id = alloc_id();
                let label = format_unit_label(st_dag, pgt_id);

                assigned_pgt_ids.insert(pgt_id);
                eu_dag.pgt_to_unit.insert(pgt_id, id);

                eu_dag.units.insert(
                    id,
                    ExecutionUnit {
                        id,
                        kind: ExecutionUnitKind::Singleton,
                        member_pgt_ids: vec![pgt_id],
                        root_pgt_id: pgt_id,
                        label,
                    },
                );
            } else {
                // Multi-member atomic group.
                let id = alloc_id();
                let root = pgt_ids[0];
                let label = format!("atomic_group({})", format_unit_members(st_dag, &pgt_ids));

                for &pid in &pgt_ids {
                    assigned_pgt_ids.insert(pid);
                    eu_dag.pgt_to_unit.insert(pid, id);
                }

                eu_dag.units.insert(
                    id,
                    ExecutionUnit {
                        id,
                        kind: ExecutionUnitKind::AtomicGroup,
                        member_pgt_ids: pgt_ids,
                        root_pgt_id: root,
                        label,
                    },
                );
            }
        }

        // Step 4: Build inter-unit dependency edges.
        // For each ST, look at its upstream edges. If an upstream ST belongs
        // to a different execution unit, add a unit→unit edge.
        let mut edge_set: HashSet<(ExecutionUnitId, ExecutionUnitId)> = HashSet::new();

        for (&pgt_id, &unit_id) in &eu_dag.pgt_to_unit {
            let node = NodeId::StreamTable(pgt_id);
            for upstream in st_dag.get_upstream(node) {
                if let NodeId::StreamTable(up_id) = upstream
                    && let Some(&up_unit_id) = eu_dag.pgt_to_unit.get(&up_id)
                    && up_unit_id != unit_id
                    && edge_set.insert((up_unit_id, unit_id))
                {
                    eu_dag.edges.entry(up_unit_id).or_default().push(unit_id);
                    eu_dag
                        .reverse_edges
                        .entry(unit_id)
                        .or_default()
                        .push(up_unit_id);
                }
            }
        }

        // Step 5 (DAG-4): Detect and fuse single-consumer CALCULATED chains.
        // A chain A → B (→ C …) is fusable when each upstream unit is a
        // Singleton with exactly one downstream, and both upstream and
        // downstream are CALCULATED (deferred) refresh mode.
        let chains = find_fusable_chains(&eu_dag, &resolve_mode);
        for chain in chains {
            eu_dag.fuse_chain(&chain);
        }

        eu_dag
    }

    /// DAG-4: Merge a chain of Singleton unit IDs into a single `FusedChain`.
    ///
    /// `chain` must contain at least 2 unit IDs in topological order
    /// (upstream-first).  The first unit's ID is reused for the fused unit;
    /// the remaining units are removed and their edges rewired.
    fn fuse_chain(&mut self, chain: &[ExecutionUnitId]) {
        if chain.len() < 2 {
            return;
        }

        let fused_id = chain[0];

        // Collect member pgt_ids in topological order across the whole chain.
        let mut member_pgt_ids: Vec<i64> = Vec::new();
        for &uid in chain {
            if let Some(unit) = self.units.get(&uid) {
                member_pgt_ids.extend_from_slice(&unit.member_pgt_ids);
            }
        }

        let root_pgt_id = member_pgt_ids[0];
        let label = format!(
            "fused_chain({})",
            member_pgt_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        // Collect external downstream edges of the LAST unit in the chain.
        let last_id = match chain.last() {
            Some(&id) => id,
            // SAFETY: chain is always non-empty by construction in build_execution_units
            None => pgrx::error!(
                "unreachable: chain is always non-empty by construction in build_execution_units"
            ),
        };
        let external_downstream: Vec<ExecutionUnitId> =
            self.edges.get(&last_id).cloned().unwrap_or_default();

        // Collect external upstream edges of the FIRST unit in the chain.
        let external_upstream: Vec<ExecutionUnitId> = self
            .reverse_edges
            .get(&fused_id)
            .cloned()
            .unwrap_or_default();

        // Remove all chain units from graphs.
        for &uid in chain {
            self.units.remove(&uid);
            self.edges.remove(&uid);
            self.reverse_edges.remove(&uid);
        }

        // Remove stale reverse/forward references from external units.
        for uid in chain {
            for targets in self.edges.values_mut() {
                targets.retain(|t| t != uid);
            }
            for sources in self.reverse_edges.values_mut() {
                sources.retain(|s| s != uid);
            }
        }

        // Insert the fused unit.
        self.units.insert(
            fused_id,
            ExecutionUnit {
                id: fused_id,
                kind: ExecutionUnitKind::FusedChain,
                member_pgt_ids: member_pgt_ids.clone(),
                root_pgt_id,
                label,
            },
        );

        // Rewire pgt_to_unit mapping.
        for &pgt_id in &member_pgt_ids {
            self.pgt_to_unit.insert(pgt_id, fused_id);
        }

        // Restore external edges.
        for &down_id in &external_downstream {
            self.edges.entry(fused_id).or_default().push(down_id);
            self.reverse_edges
                .entry(down_id)
                .or_default()
                .push(fused_id);
        }
        for &up_id in &external_upstream {
            self.edges.entry(up_id).or_default().push(fused_id);
            self.reverse_edges.entry(fused_id).or_default().push(up_id);
        }
    }

    /// Return all execution units.
    pub fn units(&self) -> impl Iterator<Item = &ExecutionUnit> {
        self.units.values()
    }

    /// PERF-5: O(1) lookup by execution unit ID (replaces O(n) `units().find()`).
    pub fn unit_by_id(&self, id: ExecutionUnitId) -> Option<&ExecutionUnit> {
        self.units.get(&id)
    }

    /// Return the number of execution units.
    pub fn unit_count(&self) -> usize {
        self.units.len()
    }

    /// Look up the execution unit for a given pgt_id.
    pub fn unit_for_pgt(&self, pgt_id: i64) -> Option<&ExecutionUnit> {
        self.pgt_to_unit
            .get(&pgt_id)
            .and_then(|id| self.units.get(id))
    }

    /// Return the upstream execution unit IDs for a given unit.
    pub fn get_upstream_units(&self, unit_id: ExecutionUnitId) -> Vec<ExecutionUnitId> {
        self.reverse_edges
            .get(&unit_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Return the downstream execution unit IDs for a given unit.
    pub fn get_downstream_units(&self, unit_id: ExecutionUnitId) -> Vec<ExecutionUnitId> {
        self.edges.get(&unit_id).cloned().unwrap_or_default()
    }

    /// Return execution units in topological order (upstream-first).
    ///
    /// Uses Kahn's algorithm. Returns an error if cycles are detected
    /// (which shouldn't happen if built from a valid StDag).
    pub fn topological_order(&self) -> Result<Vec<ExecutionUnitId>, PgTrickleError> {
        let mut in_degree: HashMap<ExecutionUnitId, usize> = HashMap::new();
        for &uid in self.units.keys() {
            in_degree.entry(uid).or_insert(0);
        }
        for targets in self.edges.values() {
            for &t in targets {
                *in_degree.entry(t).or_insert(0) += 1;
            }
        }

        let mut queue: VecDeque<ExecutionUnitId> = in_degree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut order = Vec::new();
        while let Some(uid) = queue.pop_front() {
            order.push(uid);
            if let Some(downstream) = self.edges.get(&uid) {
                for &ds in downstream {
                    if let Some(deg) = in_degree.get_mut(&ds) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(ds);
                        }
                    }
                }
            }
        }

        if order.len() < self.units.len() {
            Err(PgTrickleError::CycleDetected(vec![
                "execution unit DAG".to_string(),
            ]))
        } else {
            Ok(order)
        }
    }

    /// Compute the initial ready set — units with no upstream dependencies.
    pub fn initial_ready_set(&self) -> Vec<ExecutionUnitId> {
        self.units
            .keys()
            .filter(|uid| self.reverse_edges.get(uid).is_none_or(|ups| ups.is_empty()))
            .copied()
            .collect()
    }

    /// Return execution units grouped by parallelism level (upstream first).
    ///
    /// Level 0 contains all zero-indegree units (no upstream dependencies),
    /// level 1 contains units whose dependencies are all in level 0, etc.
    /// Units within the same level can be dispatched in parallel.
    pub fn topological_levels(&self) -> Result<Vec<Vec<ExecutionUnitId>>, PgTrickleError> {
        let mut in_degree: HashMap<ExecutionUnitId, usize> = HashMap::new();
        for &uid in self.units.keys() {
            in_degree.entry(uid).or_insert(0);
        }
        for targets in self.edges.values() {
            for &t in targets {
                *in_degree.entry(t).or_insert(0) += 1;
            }
        }

        let mut levels: Vec<Vec<ExecutionUnitId>> = Vec::new();
        let mut processed = 0usize;

        let mut current_level: Vec<ExecutionUnitId> = in_degree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(&id, _)| id)
            .collect();

        while !current_level.is_empty() {
            levels.push(current_level.clone());
            processed += current_level.len();

            let mut next_level = Vec::new();
            for &uid in &current_level {
                if let Some(downstream) = self.edges.get(&uid) {
                    for &ds in downstream {
                        if let Some(deg) = in_degree.get_mut(&ds) {
                            *deg -= 1;
                            if *deg == 0 {
                                next_level.push(ds);
                            }
                        }
                    }
                }
            }
            current_level = next_level;
        }

        if processed < self.units.len() {
            Err(PgTrickleError::CycleDetected(vec![
                "execution unit DAG".to_string(),
            ]))
        } else {
            Ok(levels)
        }
    }

    /// Format a human-readable summary of the execution unit DAG for logging.
    pub fn summary(&self) -> String {
        let mut singletons = 0u32;
        let mut atomic_groups = 0u32;
        let mut immediate_closures = 0u32;
        let mut cyclic_sccs = 0u32;
        let mut fused_chains = 0u32;
        let mut total_sts = 0usize;

        for unit in self.units.values() {
            total_sts += unit.member_pgt_ids.len();
            match unit.kind {
                ExecutionUnitKind::Singleton => singletons += 1,
                ExecutionUnitKind::AtomicGroup => atomic_groups += 1,
                ExecutionUnitKind::ImmediateClosure => immediate_closures += 1,
                ExecutionUnitKind::RepeatableReadGroup => {}
                ExecutionUnitKind::CyclicScc => cyclic_sccs += 1,
                ExecutionUnitKind::FusedChain => fused_chains += 1,
            }
        }

        let ready = self.initial_ready_set().len();
        let edges: usize = self.edges.values().map(|v| v.len()).sum();
        let n_levels = self.topological_levels().map(|l| l.len()).unwrap_or(0);

        if fused_chains > 0 {
            format!(
                "{} units ({} singleton, {} atomic, {} immediate, {} cyclic SCCs, {} fused chains), {} STs, {} edges, {} levels, {} initially ready",
                self.units.len(),
                singletons,
                atomic_groups,
                immediate_closures,
                cyclic_sccs,
                fused_chains,
                total_sts,
                edges,
                n_levels,
                ready,
            )
        } else {
            format!(
                "{} units ({} singleton, {} atomic, {} immediate, {} cyclic SCCs), {} STs, {} edges, {} levels, {} initially ready",
                self.units.len(),
                singletons,
                atomic_groups,
                immediate_closures,
                cyclic_sccs,
                total_sts,
                edges,
                n_levels,
                ready,
            )
        }
    }

    /// Format a detailed log of all units and their dependencies for dry-run mode.
    pub fn dry_run_log(&self) -> String {
        let mut out = String::new();
        out.push_str("Execution Unit DAG (dry_run):\n");

        match self.topological_order() {
            Ok(order) => {
                for (pos, uid) in order.iter().enumerate() {
                    if let Some(unit) = self.units.get(uid) {
                        let upstream = self.get_upstream_units(*uid);
                        let up_labels: Vec<String> = upstream
                            .iter()
                            .filter_map(|up| self.units.get(up).map(|u| u.label.clone()))
                            .collect();

                        out.push_str(&format!(
                            "  [{}] {} ({}) pgt_ids={:?} upstream=[{}]\n",
                            pos,
                            unit.label,
                            unit.kind,
                            unit.member_pgt_ids,
                            up_labels.join(", "),
                        ));
                    }
                }
            }
            Err(e) => {
                out.push_str(&format!("  ERROR: {}\n", e));
            }
        }

        let ready = self.initial_ready_set();
        let ready_labels: Vec<String> = ready
            .iter()
            .filter_map(|uid| self.units.get(uid).map(|u| u.label.clone()))
            .collect();
        out.push_str(&format!("  Ready queue: [{}]\n", ready_labels.join(", ")));

        out
    }

    /// Build an `ExecutionUnitDag` from a flat list of `ExecutionUnit`s with no
    /// edges.  For unit tests only — lets tests construct minimal dags without
    /// going through `build_from_st_dag`.
    #[cfg(test)]
    pub fn from_units_for_test(units: Vec<ExecutionUnit>) -> Self {
        let mut dag = ExecutionUnitDag {
            units: HashMap::new(),
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            pgt_to_unit: HashMap::new(),
        };
        for unit in units {
            for &pgt_id in &unit.member_pgt_ids {
                dag.pgt_to_unit.insert(pgt_id, unit.id);
            }
            dag.units.insert(unit.id, unit);
        }
        dag
    }
}

/// Recursively collect IMMEDIATE-mode stream tables reachable from `start`.
fn collect_immediate_closure(
    st_dag: &StDag,
    immediate_ids: &HashSet<i64>,
    assigned: &HashSet<i64>,
    start: i64,
    result: &mut Vec<i64>,
) {
    if result.contains(&start) || assigned.contains(&start) {
        return;
    }
    result.push(start);

    // Also collect further downstream IMMEDIATE nodes
    let node = NodeId::StreamTable(start);
    for downstream in st_dag.get_downstream(node) {
        if let NodeId::StreamTable(ds_id) = downstream
            && immediate_ids.contains(&ds_id)
        {
            collect_immediate_closure(st_dag, immediate_ids, assigned, ds_id, result);
        }
    }
}

/// Format a human-readable label for a single ST unit.
fn format_unit_label(st_dag: &StDag, pgt_id: i64) -> String {
    let node = NodeId::StreamTable(pgt_id);
    for n in st_dag.get_all_st_nodes() {
        if n.id == node {
            return n.name.clone();
        }
    }
    format!("pgt_{}", pgt_id)
}

/// Format a comma-separated list of ST names for compound unit labels.
fn format_unit_members(st_dag: &StDag, pgt_ids: &[i64]) -> String {
    pgt_ids
        .iter()
        .map(|&id| format_unit_label(st_dag, id))
        .collect::<Vec<_>>()
        .join(", ")
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topological_sort_simple_chain() {
        // base_table -> st1 -> st2
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(120)),
            effective_schedule: Duration::from_secs(120),
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);

        let order = dag.topological_order().unwrap();
        assert_eq!(order, vec![st1, st2]);
    }

    #[test]
    fn test_cycle_detection_detects_cycle() {
        let mut dag = StDag::new();
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(st1, st2);
        dag.add_edge(st2, st1);

        let result = dag.detect_cycles();
        assert!(result.is_err());
        if let Err(PgTrickleError::CycleDetected(nodes)) = result {
            assert_eq!(nodes.len(), 2);
        }
    }

    #[test]
    fn test_no_cycle_in_valid_dag() {
        let mut dag = StDag::new();
        let base1 = NodeId::BaseTable(1);
        let base2 = NodeId::BaseTable(2);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        for (id, name) in [(st1, "st1"), (st2, "st2"), (st3, "st3")] {
            dag.add_st_node(DagNode {
                id,
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: name.to_string(),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }

        // Diamond: base1 -> st1, base2 -> st2, st1 -> st3, st2 -> st3
        dag.add_edge(base1, st1);
        dag.add_edge(base2, st2);
        dag.add_edge(st1, st3);
        dag.add_edge(st2, st3);

        assert!(dag.detect_cycles().is_ok());
        let order = dag.topological_order().unwrap();
        // st3 must come after st1 and st2
        let pos1 = order.iter().position(|n| *n == st1).unwrap();
        let pos2 = order.iter().position(|n| *n == st2).unwrap();
        let pos3 = order.iter().position(|n| *n == st3).unwrap();
        assert!(pos3 > pos1);
        assert!(pos3 > pos2);
    }

    #[test]
    fn test_calculated_schedule_resolution() {
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        // st1 is CALCULATED (schedule = None), st2 has explicit 120s schedule
        dag.add_st_node(DagNode {
            id: st1,
            schedule: None,
            effective_schedule: Duration::ZERO,
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(120)),
            effective_schedule: Duration::from_secs(120),
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);

        dag.resolve_calculated_schedule(60);

        // st1's effective schedule should be MIN of st2's effective schedule = 120s
        let st1_node = dag.nodes.get(&st1).unwrap();
        assert_eq!(st1_node.effective_schedule, Duration::from_secs(120));
    }

    #[test]
    fn test_calculated_schedule_no_dependents_uses_fallback() {
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: None,
            effective_schedule: Duration::ZERO,
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);

        dag.resolve_calculated_schedule(60);

        // No dependents → fallback = 60s
        let st1_node = dag.nodes.get(&st1).unwrap();
        assert_eq!(st1_node.effective_schedule, Duration::from_secs(60));
    }

    #[test]
    fn test_empty_dag() {
        let dag = StDag::new();
        assert!(dag.detect_cycles().is_ok());
        assert!(dag.topological_order().unwrap().is_empty());
    }

    // ── Phase 4: Edge-case tests ────────────────────────────────────

    #[test]
    fn test_single_node_no_edges() {
        let mut dag = StDag::new();
        let st = NodeId::StreamTable(1);
        dag.add_st_node(DagNode {
            id: st,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        assert!(dag.detect_cycles().is_ok());
        let order = dag.topological_order().unwrap();
        assert_eq!(order, vec![st]);
    }

    #[test]
    fn test_get_upstream_and_downstream() {
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);

        assert_eq!(dag.get_upstream(st1), vec![base]);
        assert_eq!(dag.get_downstream(st1), vec![st2]);
        assert_eq!(dag.get_upstream(st2), vec![st1]);
        assert!(dag.get_downstream(st2).is_empty());
        assert!(dag.get_upstream(base).is_empty());
    }

    #[test]
    fn test_get_all_st_nodes() {
        let mut dag = StDag::new();
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(30)),
            effective_schedule: Duration::from_secs(30),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st2".to_string(),
            status: StStatus::Suspended,
            schedule_raw: None,
        });

        let nodes = dag.get_all_st_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_node_name_for_known_and_unknown_nodes() {
        let mut dag = StDag::new();
        let st = NodeId::StreamTable(42);
        dag.add_st_node(DagNode {
            id: st,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "my_st".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        // Known node returns its name
        assert_eq!(dag.node_name(&st), "my_st");

        // Unknown ST returns formatted string
        let unknown_st = NodeId::StreamTable(999);
        assert_eq!(dag.node_name(&unknown_st), "stream_table(id=999)");

        // Unknown base table returns formatted string
        let base = NodeId::BaseTable(123);
        assert_eq!(dag.node_name(&base), "base_table(oid=123)");
    }

    #[test]
    fn test_diamond_dependency_pattern() {
        // base1 → st1 → st3
        // base2 → st2 → st3
        let mut dag = StDag::new();
        let base1 = NodeId::BaseTable(1);
        let base2 = NodeId::BaseTable(2);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        for (id, name) in [(st1, "st1"), (st2, "st2"), (st3, "st3")] {
            dag.add_st_node(DagNode {
                id,
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: name.to_string(),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }

        dag.add_edge(base1, st1);
        dag.add_edge(base2, st2);
        dag.add_edge(st1, st3);
        dag.add_edge(st2, st3);

        assert!(dag.detect_cycles().is_ok());
        let order = dag.topological_order().unwrap();
        assert_eq!(order.len(), 3);

        // st3 must be after st1 and st2
        let pos = |id: NodeId| order.iter().position(|n| *n == id).unwrap();
        assert!(pos(st3) > pos(st1));
        assert!(pos(st3) > pos(st2));
    }

    #[test]
    fn test_pgt_status_as_str_and_from_str_roundtrip() {
        for status in [
            StStatus::Initializing,
            StStatus::Active,
            StStatus::Suspended,
            StStatus::Error,
        ] {
            let s = status.as_str();
            let parsed = StStatus::from_str(s).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_pgt_status_from_str_unknown_returns_error() {
        let result = StStatus::from_str("UNKNOWN");
        assert!(result.is_err());
        if let Err(PgTrickleError::InvalidArgument(msg)) = result {
            assert!(msg.contains("unknown status"));
        }
    }

    #[test]
    fn test_refresh_mode_as_str_and_from_str_roundtrip() {
        for mode in [
            RefreshMode::Full,
            RefreshMode::Differential,
            RefreshMode::Immediate,
        ] {
            let s = mode.as_str();
            let parsed = RefreshMode::from_str(s).unwrap();
            assert_eq!(parsed, mode);
        }
    }

    #[test]
    fn test_refresh_mode_from_str_case_insensitive() {
        assert_eq!(RefreshMode::from_str("full").unwrap(), RefreshMode::Full);
        assert_eq!(RefreshMode::from_str("FULL").unwrap(), RefreshMode::Full);
        assert_eq!(RefreshMode::from_str("Full").unwrap(), RefreshMode::Full);
        assert_eq!(
            RefreshMode::from_str("incremental").unwrap(),
            RefreshMode::Differential
        );
        assert_eq!(
            RefreshMode::from_str("IMMEDIATE").unwrap(),
            RefreshMode::Immediate
        );
        assert_eq!(
            RefreshMode::from_str("immediate").unwrap(),
            RefreshMode::Immediate
        );
    }

    #[test]
    fn test_refresh_mode_from_str_unknown_returns_error() {
        let result = RefreshMode::from_str("INVALID");
        assert!(result.is_err());
        if let Err(PgTrickleError::InvalidArgument(msg)) = result {
            assert!(msg.contains("unknown refresh mode"));
        }
    }

    #[test]
    fn test_refresh_mode_auto_resolves_to_differential() {
        assert_eq!(
            RefreshMode::from_str("AUTO").unwrap(),
            RefreshMode::Differential
        );
        assert_eq!(
            RefreshMode::from_str("auto").unwrap(),
            RefreshMode::Differential
        );
        assert_eq!(
            RefreshMode::from_str("Auto").unwrap(),
            RefreshMode::Differential
        );
    }

    #[test]
    fn test_refresh_mode_is_auto_str() {
        assert!(RefreshMode::is_auto_str("AUTO"));
        assert!(RefreshMode::is_auto_str("auto"));
        assert!(RefreshMode::is_auto_str("Auto"));
        assert!(!RefreshMode::is_auto_str("DIFFERENTIAL"));
        assert!(!RefreshMode::is_auto_str("FULL"));
    }

    #[test]
    fn test_refresh_mode_immediate_helpers() {
        assert!(RefreshMode::Immediate.is_immediate());
        assert!(!RefreshMode::Immediate.is_scheduled());
        assert!(!RefreshMode::Full.is_immediate());
        assert!(RefreshMode::Full.is_scheduled());
        assert!(!RefreshMode::Differential.is_immediate());
        assert!(RefreshMode::Differential.is_scheduled());
    }

    #[test]
    fn test_downstream_schedule_multiple_downstream_uses_minimum() {
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st_downstream = NodeId::StreamTable(1);
        let st_fast = NodeId::StreamTable(2);
        let st_slow = NodeId::StreamTable(3);

        // st_downstream (DOWNSTREAM) → st_fast (30s), st_slow (120s)
        dag.add_st_node(DagNode {
            id: st_downstream,
            schedule: None,
            effective_schedule: Duration::ZERO,
            name: "st_downstream".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st_fast,
            schedule: Some(Duration::from_secs(30)),
            effective_schedule: Duration::from_secs(30),
            name: "st_fast".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st_slow,
            schedule: Some(Duration::from_secs(120)),
            effective_schedule: Duration::from_secs(120),
            name: "st_slow".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st_downstream);
        dag.add_edge(st_downstream, st_fast);
        dag.add_edge(st_downstream, st_slow);

        dag.resolve_calculated_schedule(60);

        // Should use MIN(30, 120) = 30
        let node = dag.nodes.get(&st_downstream).unwrap();
        assert_eq!(node.effective_schedule, Duration::from_secs(30));
    }

    #[test]
    fn test_default_trait_for_st_dag() {
        let dag = StDag::default();
        assert!(dag.detect_cycles().is_ok());
        assert!(dag.topological_order().unwrap().is_empty());
    }

    #[test]
    fn test_node_id_equality_and_hashing() {
        use std::collections::HashSet;
        let a = NodeId::BaseTable(1);
        let b = NodeId::BaseTable(1);
        let c = NodeId::StreamTable(1);
        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b); // same as a
        set.insert(c);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_cycle_detection_three_node_cycle() {
        let mut dag = StDag::new();
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        for (id, name) in [(st1, "st1"), (st2, "st2"), (st3, "st3")] {
            dag.add_st_node(DagNode {
                id,
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: name.to_string(),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }

        dag.add_edge(st1, st2);
        dag.add_edge(st2, st3);
        dag.add_edge(st3, st1);

        let result = dag.detect_cycles();
        assert!(result.is_err());
        if let Err(PgTrickleError::CycleDetected(nodes)) = result {
            assert_eq!(nodes.len(), 3);
        }
    }

    #[test]
    fn test_topological_order_excludes_base_tables() {
        let mut dag = StDag::new();
        let base1 = NodeId::BaseTable(1);
        let base2 = NodeId::BaseTable(2);
        let st1 = NodeId::StreamTable(1);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base1, st1);
        dag.add_edge(base2, st1);

        let order = dag.topological_order().unwrap();
        assert_eq!(order, vec![st1]); // No base tables in output
    }

    #[test]
    fn test_explicit_schedule_overrides_downstream_resolution() {
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        // st1 has explicit schedule of 60s, st2 has explicit schedule of 120s
        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::ZERO, // will be set to 60
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(120)),
            effective_schedule: Duration::ZERO, // will be set to 120
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);

        dag.resolve_calculated_schedule(30);

        assert_eq!(
            dag.nodes.get(&st1).unwrap().effective_schedule,
            Duration::from_secs(60)
        );
        assert_eq!(
            dag.nodes.get(&st2).unwrap().effective_schedule,
            Duration::from_secs(120)
        );
    }

    #[test]
    fn test_resolve_downstream_schedule_chain_three_levels() {
        // base -> st1 (downstream) -> st2 (downstream) -> st3 (60s)
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: None, // downstream
            effective_schedule: Duration::ZERO,
            name: "st1".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: None, // downstream
            effective_schedule: Duration::ZERO,
            name: "st2".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st3,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: "st3".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);
        dag.add_edge(st2, st3);

        dag.resolve_calculated_schedule(300);

        // st2 gets min of st3 = 60s, st1 gets min of st2 = 60s
        assert_eq!(
            dag.nodes.get(&st1).unwrap().effective_schedule,
            Duration::from_secs(60)
        );
        assert_eq!(
            dag.nodes.get(&st2).unwrap().effective_schedule,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_node_name_unknown_base_table() {
        let dag = StDag::new();
        // Node not in the graph → should produce a fallback name
        let name = dag.node_name(&NodeId::BaseTable(99999));
        assert!(name.contains("99999"), "expected OID in name: {}", name);
        assert!(
            name.contains("base_table"),
            "expected 'base_table' prefix: {}",
            name
        );
    }

    #[test]
    fn test_node_name_unknown_stream_table() {
        let dag = StDag::new();
        let name = dag.node_name(&NodeId::StreamTable(42));
        assert!(name.contains("42"), "expected ID in name: {}", name);
        assert!(
            name.contains("stream_table"),
            "expected 'stream_table' prefix: {}",
            name
        );
    }

    #[test]
    fn test_cycle_detection_error_message_contains_node_names() {
        let mut dag = StDag::new();
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);

        dag.add_st_node(DagNode {
            id: st1,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::ZERO,
            name: "my_view_a".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        dag.add_st_node(DagNode {
            id: st2,
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::ZERO,
            name: "my_view_b".to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        });

        dag.add_edge(st1, st2);
        dag.add_edge(st2, st1); // cycle!

        let err = dag.detect_cycles().unwrap_err();
        let msg = format!("{}", err);
        // Error message should reference the named nodes
        assert!(
            msg.contains("my_view_a") || msg.contains("my_view_b"),
            "cycle error should contain node names: {}",
            msg
        );
    }

    // ── Diamond detection tests ─────────────────────────────────────────

    /// Helper: create a simple DagNode for tests.
    fn make_st(id: i64, name: &str) -> DagNode {
        DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: name.to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        }
    }

    #[test]
    fn test_detect_diamonds_simple() {
        // A (base) → B (ST) → D (ST)
        // A (base) → C (ST) → D (ST)
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));
        dag.add_st_node(make_st(3, "D"));

        dag.add_edge(a, b);
        dag.add_edge(a, c);
        dag.add_edge(b, d);
        dag.add_edge(c, d);

        let diamonds = dag.detect_diamonds();
        assert_eq!(diamonds.len(), 1, "expected one diamond, got {diamonds:?}");
        assert_eq!(diamonds[0].convergence, d);
        assert!(
            diamonds[0].shared_sources.contains(&a),
            "shared sources should contain A"
        );
        let int_set: HashSet<NodeId> = diamonds[0].intermediates.iter().copied().collect();
        assert!(int_set.contains(&b), "intermediates should contain B");
        assert!(int_set.contains(&c), "intermediates should contain C");
    }

    #[test]
    fn test_detect_diamonds_deep() {
        // A (base) → B → E → D
        // A (base) → C → D
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);
        let e = NodeId::StreamTable(4);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));
        dag.add_st_node(make_st(3, "D"));
        dag.add_st_node(make_st(4, "E"));

        dag.add_edge(a, b);
        dag.add_edge(b, e);
        dag.add_edge(e, d);
        dag.add_edge(a, c);
        dag.add_edge(c, d);

        let diamonds = dag.detect_diamonds();
        assert_eq!(diamonds.len(), 1, "expected one diamond, got {diamonds:?}");
        assert_eq!(diamonds[0].convergence, d);

        let int_set: HashSet<NodeId> = diamonds[0].intermediates.iter().copied().collect();
        // B, C, and E are all intermediates
        assert!(int_set.contains(&b), "should contain B");
        assert!(int_set.contains(&c), "should contain C");
        assert!(int_set.contains(&e), "should contain E");
    }

    #[test]
    fn test_detect_diamonds_none_linear() {
        // Linear chain: A → B → C (no diamond)
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));

        dag.add_edge(a, b);
        dag.add_edge(b, c);

        let diamonds = dag.detect_diamonds();
        assert!(diamonds.is_empty(), "linear chain should have no diamonds");
    }

    #[test]
    fn test_detect_diamonds_multiple_roots_no_diamond() {
        // B1 (base) → B2 → D
        // C1 (base) → C2 → D
        // Different roots — NOT a diamond (cross-source, not intra-source).
        let mut dag = StDag::new();
        let b1 = NodeId::BaseTable(1);
        let c1 = NodeId::BaseTable(2);
        let b2 = NodeId::StreamTable(1);
        let c2 = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);

        dag.add_st_node(make_st(1, "B2"));
        dag.add_st_node(make_st(2, "C2"));
        dag.add_st_node(make_st(3, "D"));

        dag.add_edge(b1, b2);
        dag.add_edge(c1, c2);
        dag.add_edge(b2, d);
        dag.add_edge(c2, d);

        let diamonds = dag.detect_diamonds();
        assert!(
            diamonds.is_empty(),
            "different-root fan-in should not be a diamond"
        );
    }

    #[test]
    fn test_detect_diamonds_overlapping() {
        // A → B → D,  A → C → D   (diamond 1)
        // D → E → G,  D → F → G   (diamond 2, overlapping at D)
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);
        let e = NodeId::StreamTable(4);
        let f = NodeId::StreamTable(5);
        let g = NodeId::StreamTable(6);

        for (id, name) in [(1, "B"), (2, "C"), (3, "D"), (4, "E"), (5, "F"), (6, "G")] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(a, b);
        dag.add_edge(a, c);
        dag.add_edge(b, d);
        dag.add_edge(c, d);
        dag.add_edge(d, e);
        dag.add_edge(d, f);
        dag.add_edge(e, g);
        dag.add_edge(f, g);

        let diamonds = dag.detect_diamonds();
        // Should detect 2 diamonds (convergence at D and at G).
        // They may or may not be merged depending on intermediate overlap.
        assert!(!diamonds.is_empty(), "should detect at least one diamond");

        // Check that both D and G are convergence or at least one diamond
        // captures both fan-in points.
        let convergence_nodes: HashSet<NodeId> = diamonds.iter().map(|d| d.convergence).collect();
        assert!(
            convergence_nodes.contains(&d) || convergence_nodes.contains(&g),
            "should detect diamond at D or G"
        );
    }

    // ── Consistency group tests ─────────────────────────────────────────

    #[test]
    fn test_groups_simple_diamond() {
        // A → B → D, A → C → D
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));
        dag.add_st_node(make_st(3, "D"));

        dag.add_edge(a, b);
        dag.add_edge(a, c);
        dag.add_edge(b, d);
        dag.add_edge(c, d);

        let groups = dag.compute_consistency_groups();

        // Find the non-singleton group containing D.
        let diamond_group = groups.iter().find(|g| !g.is_singleton());
        assert!(
            diamond_group.is_some(),
            "expected a non-singleton group for the diamond"
        );
        let group = diamond_group.unwrap();

        assert!(group.members.contains(&b), "group should contain B");
        assert!(group.members.contains(&c), "group should contain C");
        assert!(group.members.contains(&d), "group should contain D");
        assert!(
            group.convergence_points.contains(&d),
            "D should be a convergence point"
        );

        // D must be last in topological order within the group.
        assert_eq!(
            *group.members.last().unwrap(),
            d,
            "convergence node D should be last"
        );
    }

    #[test]
    fn test_groups_singleton_non_diamond() {
        // Linear: A → B → C (no diamond)
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));

        dag.add_edge(a, b);
        dag.add_edge(b, c);

        let groups = dag.compute_consistency_groups();
        assert_eq!(
            groups.len(),
            2,
            "each ST should get its own singleton group"
        );
        assert!(groups.iter().all(|g| g.is_singleton()));
    }

    #[test]
    fn test_groups_independent_diamonds() {
        // Diamond 1: A1 → B1 → D1, A1 → C1 → D1
        // Diamond 2: A2 → B2 → D2, A2 → C2 → D2
        let mut dag = StDag::new();
        let a1 = NodeId::BaseTable(1);
        let a2 = NodeId::BaseTable(2);
        let b1 = NodeId::StreamTable(1);
        let c1 = NodeId::StreamTable(2);
        let d1 = NodeId::StreamTable(3);
        let b2 = NodeId::StreamTable(4);
        let c2 = NodeId::StreamTable(5);
        let d2 = NodeId::StreamTable(6);

        for (id, name) in [
            (1, "B1"),
            (2, "C1"),
            (3, "D1"),
            (4, "B2"),
            (5, "C2"),
            (6, "D2"),
        ] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(a1, b1);
        dag.add_edge(a1, c1);
        dag.add_edge(b1, d1);
        dag.add_edge(c1, d1);

        dag.add_edge(a2, b2);
        dag.add_edge(a2, c2);
        dag.add_edge(b2, d2);
        dag.add_edge(c2, d2);

        let groups = dag.compute_consistency_groups();
        let non_singleton: Vec<_> = groups.iter().filter(|g| !g.is_singleton()).collect();
        assert_eq!(
            non_singleton.len(),
            2,
            "two independent diamonds should produce two non-singleton groups"
        );

        // Each non-singleton group should have 3 members.
        for g in &non_singleton {
            assert_eq!(g.members.len(), 3, "diamond group should have 3 members");
        }
    }

    #[test]
    fn test_groups_nested_merge() {
        // A → B → D, A → C → D (diamond at D)
        // D → E → G, D → F → G (diamond at G)
        // These should merge because D is in both.
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);
        let d = NodeId::StreamTable(3);
        let e = NodeId::StreamTable(4);
        let f = NodeId::StreamTable(5);
        let g = NodeId::StreamTable(6);

        for (id, name) in [(1, "B"), (2, "C"), (3, "D"), (4, "E"), (5, "F"), (6, "G")] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(a, b);
        dag.add_edge(a, c);
        dag.add_edge(b, d);
        dag.add_edge(c, d);
        dag.add_edge(d, e);
        dag.add_edge(d, f);
        dag.add_edge(e, g);
        dag.add_edge(f, g);

        let groups = dag.compute_consistency_groups();
        let non_singleton: Vec<_> = groups.iter().filter(|g| !g.is_singleton()).collect();

        // D is shared between both diamonds, so groups should be merged.
        // Total non-singleton group(s) should contain B, C, D, E, F, G.
        let all_members: HashSet<NodeId> = non_singleton
            .iter()
            .flat_map(|g| g.members.iter().copied())
            .collect();

        assert!(all_members.contains(&b));
        assert!(all_members.contains(&c));
        assert!(all_members.contains(&d));
        assert!(all_members.contains(&e));
        assert!(all_members.contains(&f));
        assert!(all_members.contains(&g));
    }

    #[test]
    fn test_consistency_group_epoch_advance() {
        let mut group = ConsistencyGroup {
            members: vec![NodeId::StreamTable(1), NodeId::StreamTable(2)],
            convergence_points: vec![NodeId::StreamTable(2)],
            epoch: 0,
            isolation_level: IsolationLevel::ReadCommitted,
        };

        assert_eq!(group.epoch, 0);
        group.advance_epoch();
        assert_eq!(group.epoch, 1);
        group.advance_epoch();
        assert_eq!(group.epoch, 2);
    }

    #[test]
    fn test_consistency_group_is_singleton() {
        let singleton = ConsistencyGroup {
            members: vec![NodeId::StreamTable(1)],
            convergence_points: vec![],
            epoch: 0,
            isolation_level: IsolationLevel::ReadCommitted,
        };
        assert!(singleton.is_singleton());

        let multi = ConsistencyGroup {
            members: vec![NodeId::StreamTable(1), NodeId::StreamTable(2)],
            convergence_points: vec![NodeId::StreamTable(2)],
            epoch: 0,
            isolation_level: IsolationLevel::ReadCommitted,
        };
        assert!(!multi.is_singleton());
    }

    // ── DiamondConsistency enum tests ──────────────────────────────────

    #[test]
    fn test_diamond_consistency_as_str() {
        assert_eq!(DiamondConsistency::None.as_str(), "none");
        assert_eq!(DiamondConsistency::Atomic.as_str(), "atomic");
    }

    #[test]
    fn test_diamond_consistency_from_sql_str() {
        assert_eq!(
            DiamondConsistency::from_sql_str("none"),
            DiamondConsistency::None
        );
        assert_eq!(
            DiamondConsistency::from_sql_str("atomic"),
            DiamondConsistency::Atomic
        );
        assert_eq!(
            DiamondConsistency::from_sql_str("ATOMIC"),
            DiamondConsistency::Atomic
        );
        assert_eq!(
            DiamondConsistency::from_sql_str("NONE"),
            DiamondConsistency::None
        );
        // Unknown values fall back to None
        assert_eq!(
            DiamondConsistency::from_sql_str("unknown"),
            DiamondConsistency::None
        );
    }

    #[test]
    fn test_diamond_consistency_display() {
        assert_eq!(format!("{}", DiamondConsistency::None), "none");
        assert_eq!(format!("{}", DiamondConsistency::Atomic), "atomic");
    }

    #[test]
    fn test_diamond_consistency_roundtrip() {
        for dc in [DiamondConsistency::None, DiamondConsistency::Atomic] {
            let s = dc.as_str();
            let parsed = DiamondConsistency::from_sql_str(s);
            assert_eq!(dc, parsed);
        }
    }

    // ── DiamondSchedulePolicy enum tests ───────────────────────────────

    #[test]
    fn test_diamond_schedule_policy_from_str() {
        assert_eq!(
            DiamondSchedulePolicy::from_sql_str("fastest"),
            Some(DiamondSchedulePolicy::Fastest)
        );
        assert_eq!(
            DiamondSchedulePolicy::from_sql_str("slowest"),
            Some(DiamondSchedulePolicy::Slowest)
        );
        assert_eq!(
            DiamondSchedulePolicy::from_sql_str("FASTEST"),
            Some(DiamondSchedulePolicy::Fastest)
        );
        assert_eq!(
            DiamondSchedulePolicy::from_sql_str("SLOWEST"),
            Some(DiamondSchedulePolicy::Slowest)
        );
        assert_eq!(
            DiamondSchedulePolicy::from_sql_str(" fastest "),
            Some(DiamondSchedulePolicy::Fastest)
        );
        // Invalid values return None
        assert_eq!(DiamondSchedulePolicy::from_sql_str("invalid"), None);
        assert_eq!(DiamondSchedulePolicy::from_sql_str(""), None);
    }

    #[test]
    fn test_diamond_schedule_policy_default() {
        assert_eq!(
            DiamondSchedulePolicy::default(),
            DiamondSchedulePolicy::Fastest
        );
    }

    #[test]
    fn test_diamond_schedule_policy_as_str() {
        assert_eq!(DiamondSchedulePolicy::Fastest.as_str(), "fastest");
        assert_eq!(DiamondSchedulePolicy::Slowest.as_str(), "slowest");
    }

    #[test]
    fn test_diamond_schedule_policy_stricter() {
        // Slowest is always the stricter policy
        assert_eq!(
            DiamondSchedulePolicy::Slowest.stricter(DiamondSchedulePolicy::Fastest),
            DiamondSchedulePolicy::Slowest
        );
        assert_eq!(
            DiamondSchedulePolicy::Fastest.stricter(DiamondSchedulePolicy::Slowest),
            DiamondSchedulePolicy::Slowest
        );
        assert_eq!(
            DiamondSchedulePolicy::Slowest.stricter(DiamondSchedulePolicy::Slowest),
            DiamondSchedulePolicy::Slowest
        );
        assert_eq!(
            DiamondSchedulePolicy::Fastest.stricter(DiamondSchedulePolicy::Fastest),
            DiamondSchedulePolicy::Fastest
        );
    }

    #[test]
    fn test_diamond_schedule_policy_display() {
        assert_eq!(format!("{}", DiamondSchedulePolicy::Fastest), "fastest");
        assert_eq!(format!("{}", DiamondSchedulePolicy::Slowest), "slowest");
    }

    #[test]
    fn test_diamond_schedule_policy_roundtrip() {
        for p in [
            DiamondSchedulePolicy::Fastest,
            DiamondSchedulePolicy::Slowest,
        ] {
            let s = p.as_str();
            let parsed = DiamondSchedulePolicy::from_sql_str(s);
            assert_eq!(Some(p), parsed);
        }
    }

    // ── ExecutionUnitDag tests ───────────────────────────────────────

    /// Helper to create a simple DagNode.
    fn make_st_node(id: i64, name: &str) -> DagNode {
        DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: name.to_string(),
            status: StStatus::Active,
            schedule_raw: None,
        }
    }

    /// Default mode resolver: everything is DIFFERENTIAL.
    fn all_differential(id: i64) -> Option<RefreshMode> {
        let _ = id;
        Some(RefreshMode::Differential)
    }

    #[test]
    fn test_eu_dag_singleton_chain() {
        // base -> st1 -> st2
        // DAG-4: Single-consumer DIFFERENTIAL chain → fused into one unit.
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // DAG-4: Fused into a single FusedChain unit.
        assert_eq!(eu.unit_count(), 1);
        let unit = eu.units().next().unwrap();
        assert_eq!(unit.kind, ExecutionUnitKind::FusedChain);
        assert_eq!(unit.member_pgt_ids, vec![1, 2]);

        // Both pgt_ids map to the same unit.
        let u1 = eu.unit_for_pgt(1).unwrap();
        let u2 = eu.unit_for_pgt(2).unwrap();
        assert_eq!(u1.id, u2.id);

        // Topological order should have just 1 unit.
        let order = eu.topological_order().unwrap();
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn test_eu_dag_independent_singletons() {
        // base1 -> st1, base2 -> st2 (no dependency between them)
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::BaseTable(200), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        assert_eq!(eu.unit_count(), 2);
        // Both initially ready (no upstream dependencies)
        let ready = eu.initial_ready_set();
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn test_eu_dag_diamond_atomic_group() {
        // Diamond: base -> st1 -> st3, base -> st2 -> st3
        // With diamond_consistency = atomic on st3
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));

        let base = NodeId::BaseTable(100);
        dag.add_edge(base, NodeId::StreamTable(1));
        dag.add_edge(base, NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // The diamond should create a multi-member group containing {st1, st2, st3}
        // and then any remaining singletons. Since all are in the diamond group,
        // we expect either one atomic group or individual singletons depending on
        // whether DiamondConsistency is Atomic. Since compute_consistency_groups
        // groups diamond members but the scheduler checks for Atomic opt-in,
        // the consistency group will have members [st1, st2, st3].
        // The EU DAG just wraps consistency groups, so this should be an
        // AtomicGroup with 3 members.
        let has_multi_member = eu.units().any(|u| u.member_pgt_ids.len() > 1);
        assert!(has_multi_member, "Expected a multi-member unit for diamond");

        let multi = eu.units().find(|u| u.member_pgt_ids.len() > 1).unwrap();
        assert_eq!(multi.kind, ExecutionUnitKind::AtomicGroup);
        assert!(multi.member_pgt_ids.contains(&1));
        assert!(multi.member_pgt_ids.contains(&2));
        assert!(multi.member_pgt_ids.contains(&3));
    }

    #[test]
    fn test_eu_dag_immediate_closure_collapse() {
        // base -> st1(DIFFERENTIAL) -> st2(IMMEDIATE) -> st3(IMMEDIATE)
        // st2 and st3 are IMMEDIATE, so they should be collapsed with st1
        // into a single ImmediateClosure unit.
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));

        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        let mode_fn = |id: i64| -> Option<RefreshMode> {
            match id {
                1 => Some(RefreshMode::Differential),
                2 | 3 => Some(RefreshMode::Immediate),
                _ => None,
            }
        };

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, mode_fn);

        // st1, st2, st3 should all be in one ImmediateClosure unit
        assert_eq!(eu.unit_count(), 1);
        let unit = eu.units().next().unwrap();
        assert_eq!(unit.kind, ExecutionUnitKind::ImmediateClosure);
        assert_eq!(unit.member_pgt_ids.len(), 3);
    }

    #[test]
    fn test_eu_dag_mixed_immediate_and_independent() {
        // base1 -> st1 -> st2(IMMEDIATE)
        // base2 -> st3 (independent)
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));

        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::BaseTable(200), NodeId::StreamTable(3));

        let mode_fn = |id: i64| -> Option<RefreshMode> {
            match id {
                2 => Some(RefreshMode::Immediate),
                _ => Some(RefreshMode::Differential),
            }
        };

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, mode_fn);

        // Should have 2 units: ImmediateClosure(st1, st2) + Singleton(st3)
        assert_eq!(eu.unit_count(), 2);

        let closure = eu.unit_for_pgt(1).unwrap();
        assert_eq!(closure.kind, ExecutionUnitKind::ImmediateClosure);
        assert!(closure.member_pgt_ids.contains(&1));
        assert!(closure.member_pgt_ids.contains(&2));

        let singleton = eu.unit_for_pgt(3).unwrap();
        assert_eq!(singleton.kind, ExecutionUnitKind::Singleton);

        // Both should be initially ready (independent)
        let ready = eu.initial_ready_set();
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn test_eu_dag_summary_format() {
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::BaseTable(200), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);
        let summary = eu.summary();

        assert!(summary.contains("2 units"));
        assert!(summary.contains("2 singleton"));
        assert!(summary.contains("2 initially ready"));
    }

    #[test]
    fn test_eu_dag_dry_run_log() {
        // Use a fan-out pattern so the chain is NOT fused (st1 has 2 downstreams).
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);
        let log = eu.dry_run_log();

        assert!(log.contains("Execution Unit DAG (dry_run)"));
        assert!(log.contains("public.st1"));
        assert!(log.contains("public.st2"));
        assert!(log.contains("public.st3"));
        assert!(log.contains("Ready queue"));
    }

    #[test]
    fn test_eu_dag_empty() {
        let dag = StDag::new();
        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);
        assert_eq!(eu.unit_count(), 0);
        assert!(eu.initial_ready_set().is_empty());
        assert!(eu.topological_order().unwrap().is_empty());
    }

    #[test]
    fn test_execution_unit_kind_display() {
        assert_eq!(ExecutionUnitKind::Singleton.as_str(), "singleton");
        assert_eq!(ExecutionUnitKind::AtomicGroup.as_str(), "atomic_group");
        assert_eq!(
            ExecutionUnitKind::ImmediateClosure.as_str(),
            "immediate_closure"
        );
    }

    #[test]
    fn test_execution_unit_id_display() {
        let id = ExecutionUnitId(42);
        assert_eq!(format!("{}", id), "eu_42");
    }

    // ── stable_key tests ───────────────────────────────────────────

    #[test]
    fn test_stable_key_singleton() {
        let unit = ExecutionUnit {
            id: ExecutionUnitId(1),
            kind: ExecutionUnitKind::Singleton,
            member_pgt_ids: vec![42],
            root_pgt_id: 42,
            label: "test".to_string(),
        };
        assert_eq!(unit.stable_key(), "s:42");
    }

    #[test]
    fn test_stable_key_atomic_group() {
        let unit = ExecutionUnit {
            id: ExecutionUnitId(2),
            kind: ExecutionUnitKind::AtomicGroup,
            member_pgt_ids: vec![10, 20, 30],
            root_pgt_id: 10,
            label: "test".to_string(),
        };
        assert_eq!(unit.stable_key(), "a:10,20,30");
    }

    #[test]
    fn test_stable_key_immediate_closure() {
        let unit = ExecutionUnit {
            id: ExecutionUnitId(3),
            kind: ExecutionUnitKind::ImmediateClosure,
            member_pgt_ids: vec![5, 15],
            root_pgt_id: 5,
            label: "test".to_string(),
        };
        assert_eq!(unit.stable_key(), "i:5,15");
    }

    #[test]
    fn test_stable_key_deterministic() {
        let unit1 = ExecutionUnit {
            id: ExecutionUnitId(1),
            kind: ExecutionUnitKind::Singleton,
            member_pgt_ids: vec![99],
            root_pgt_id: 99,
            label: "a".to_string(),
        };
        let unit2 = ExecutionUnit {
            id: ExecutionUnitId(999),
            kind: ExecutionUnitKind::Singleton,
            member_pgt_ids: vec![99],
            root_pgt_id: 99,
            label: "b".to_string(),
        };
        // Same kind + same members → same stable key, regardless of ID/label.
        assert_eq!(unit1.stable_key(), unit2.stable_key());
    }

    // ── SCC (Tarjan's algorithm) tests (CYC-1) ─────────────────────────

    #[test]
    fn test_scc_no_cycles() {
        // A → B → C: three singleton SCCs.
        let mut dag = StDag::new();
        let a = NodeId::BaseTable(1);
        let b = NodeId::StreamTable(1);
        let c = NodeId::StreamTable(2);

        dag.add_st_node(make_st(1, "B"));
        dag.add_st_node(make_st(2, "C"));

        dag.add_edge(a, b);
        dag.add_edge(b, c);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        // All SCCs should be singletons (no cycle).
        assert!(
            sccs.iter().all(|scc| !scc.is_cyclic),
            "linear chain should have no cyclic SCCs"
        );
        // Total nodes across all SCCs = 3 (a, b, c).
        let total_nodes: usize = sccs.iter().map(|scc| scc.nodes.len()).sum();
        assert_eq!(total_nodes, 3);
    }

    #[test]
    fn test_scc_simple_cycle() {
        // A → B → A: one SCC {A, B}.
        let mut dag = StDag::new();
        let a = NodeId::StreamTable(1);
        let b = NodeId::StreamTable(2);

        dag.add_st_node(make_st(1, "A"));
        dag.add_st_node(make_st(2, "B"));

        dag.add_edge(a, b);
        dag.add_edge(b, a);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        let cyclic_sccs: Vec<_> = sccs.iter().filter(|scc| scc.is_cyclic).collect();
        assert_eq!(cyclic_sccs.len(), 1, "expected one cyclic SCC");
        assert_eq!(cyclic_sccs[0].nodes.len(), 2, "SCC should contain A and B");

        // Both nodes should be in the cyclic SCC.
        let node_set: HashSet<NodeId> = cyclic_sccs[0].nodes.iter().copied().collect();
        assert!(node_set.contains(&a));
        assert!(node_set.contains(&b));
    }

    #[test]
    fn test_scc_mixed() {
        // {A, B} cycle → C singleton → {D, E} cycle
        let mut dag = StDag::new();
        let a = NodeId::StreamTable(1);
        let b = NodeId::StreamTable(2);
        let c = NodeId::StreamTable(3);
        let d = NodeId::StreamTable(4);
        let e = NodeId::StreamTable(5);

        for (id, name) in [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")] {
            dag.add_st_node(make_st(id, name));
        }

        // Cycle 1: A ↔ B
        dag.add_edge(a, b);
        dag.add_edge(b, a);
        // A,B → C
        dag.add_edge(b, c);
        // C → D, E; cycle 2: D ↔ E
        dag.add_edge(c, d);
        dag.add_edge(c, e);
        dag.add_edge(d, e);
        dag.add_edge(e, d);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        let cyclic_sccs: Vec<_> = sccs.iter().filter(|scc| scc.is_cyclic).collect();
        assert_eq!(cyclic_sccs.len(), 2, "expected two cyclic SCCs");

        // Verify condensation order: {A,B} SCC before C before {D,E} SCC.
        let scc_of = |node: NodeId| -> usize {
            sccs.iter()
                .position(|scc| scc.nodes.contains(&node))
                .unwrap()
        };
        assert!(scc_of(a) < scc_of(c), "SCC(A,B) should come before SCC(C)");
        assert!(scc_of(c) < scc_of(d), "SCC(C) should come before SCC(D,E)");
    }

    #[test]
    fn test_scc_self_loop() {
        // A → A: one SCC {A} (is_cyclic = true).
        let mut dag = StDag::new();
        let a = NodeId::StreamTable(1);
        dag.add_st_node(make_st(1, "A"));
        dag.add_edge(a, a);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        assert_eq!(sccs.len(), 1);
        assert!(sccs[0].is_cyclic, "self-loop should be cyclic");
        assert_eq!(sccs[0].nodes.len(), 1);
    }

    #[test]
    fn test_scc_three_node_cycle() {
        // A → B → C → A.
        let mut dag = StDag::new();
        let a = NodeId::StreamTable(1);
        let b = NodeId::StreamTable(2);
        let c = NodeId::StreamTable(3);

        for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(a, b);
        dag.add_edge(b, c);
        dag.add_edge(c, a);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        let cyclic_sccs: Vec<_> = sccs.iter().filter(|scc| scc.is_cyclic).collect();
        assert_eq!(cyclic_sccs.len(), 1);
        assert_eq!(cyclic_sccs[0].nodes.len(), 3);
    }

    #[test]
    fn test_condensation_order_is_topological() {
        // base → A → B, A → C (no cycles → all singletons in topological order).
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let a = NodeId::StreamTable(1);
        let b = NodeId::StreamTable(2);
        let c = NodeId::StreamTable(3);

        for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(base, a);
        dag.add_edge(a, b);
        dag.add_edge(a, c);

        let sccs = dag.condensation_order().expect("SCC invariant: test");
        // base should appear in an SCC before A, and A before B/C.
        let pos_of = |node: NodeId| -> usize {
            sccs.iter()
                .position(|scc| scc.nodes.contains(&node))
                .unwrap()
        };
        assert!(pos_of(base) < pos_of(a));
        assert!(pos_of(a) < pos_of(b));
        assert!(pos_of(a) < pos_of(c));
    }

    #[test]
    fn test_scc_empty_dag() {
        let dag = StDag::new();
        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        assert!(sccs.is_empty());
    }

    #[test]
    fn test_scc_cycle_with_tail() {
        // base → A → B → C → B (B-C form a cycle, A is upstream).
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let a = NodeId::StreamTable(1);
        let b = NodeId::StreamTable(2);
        let c = NodeId::StreamTable(3);

        for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
            dag.add_st_node(make_st(id, name));
        }

        dag.add_edge(base, a);
        dag.add_edge(a, b);
        dag.add_edge(b, c);
        dag.add_edge(c, b);

        let sccs = dag.compute_sccs().expect("SCC invariant: test");
        let cyclic_sccs: Vec<_> = sccs.iter().filter(|scc| scc.is_cyclic).collect();
        assert_eq!(cyclic_sccs.len(), 1, "B-C should form one cyclic SCC");
        assert_eq!(cyclic_sccs[0].nodes.len(), 2);

        // A (and base) should be non-cyclic singletons.
        let a_scc = sccs.iter().find(|scc| scc.nodes.contains(&a)).unwrap();
        assert!(!a_scc.is_cyclic);

        // A should come before B-C in condensation order.
        let pos_of = |node: NodeId| -> usize {
            sccs.iter()
                .position(|scc| scc.nodes.contains(&node))
                .unwrap()
        };
        assert!(pos_of(a) < pos_of(b));
    }

    // ── topological_levels tests ────────────────────────────────

    fn make_dag_node(id: i64) -> DagNode {
        DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{id}"),
            status: StStatus::Active,
            schedule_raw: None,
        }
    }

    #[test]
    fn test_topological_levels_linear_chain() {
        // base → st1 → st2 → st3  →  3 levels (one ST each)
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_st_node(make_dag_node(3));
        dag.add_edge(base, st1);
        dag.add_edge(st1, st2);
        dag.add_edge(st2, st3);

        let levels = dag.topological_levels().unwrap();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec![st1]);
        assert_eq!(levels[1], vec![st2]);
        assert_eq!(levels[2], vec![st3]);
    }

    #[test]
    fn test_topological_levels_wide_fan_out() {
        // base → st1, base → st2, base → st3  →  1 level with 3 STs
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_st_node(make_dag_node(3));
        dag.add_edge(base, st1);
        dag.add_edge(base, st2);
        dag.add_edge(base, st3);

        let levels = dag.topological_levels().unwrap();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 3);
    }

    #[test]
    fn test_topological_levels_diamond() {
        // base → st1 → st3
        // base → st2 → st3
        // Level 0: [st1, st2], Level 1: [st3]
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);

        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_st_node(make_dag_node(3));
        dag.add_edge(base, st1);
        dag.add_edge(base, st2);
        dag.add_edge(st1, st3);
        dag.add_edge(st2, st3);

        let levels = dag.topological_levels().unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 2); // st1 and st2
        assert_eq!(levels[1], vec![st3]);
    }

    #[test]
    fn test_topological_levels_empty() {
        let dag = StDag::new();
        let levels = dag.topological_levels().unwrap();
        assert!(levels.is_empty());
    }

    #[test]
    fn test_eu_dag_topological_levels_simple() {
        // base → st1, base → st2
        // st1 → st3 AND st2 → st3 (fan-in: st1 has 2 downstreams → no fusion for st1)
        // Level 0: [st1, st2], Level 1: [st3]
        //
        // Note: st1 has 2 downstreams (st3... wait no, only st3 from st1).
        // But st1 → st3 only. However neither st1 nor st2 is fused because:
        // st1 has 1 downstream (st3). st2 has 1 downstream (via a second edge
        // we add). So we need to break the fusion condition.
        //
        // Use a fan-out from st1 to prevent fusion:
        // base → st1 → st3
        //              → st4  (st1 has 2 downstreams → not fusable)
        // base → st2 (independent)
        let mut dag = StDag::new();
        let base = NodeId::BaseTable(1);
        let st1 = NodeId::StreamTable(1);
        let st2 = NodeId::StreamTable(2);
        let st3 = NodeId::StreamTable(3);
        let st4 = NodeId::StreamTable(4);

        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_st_node(make_dag_node(3));
        dag.add_st_node(make_dag_node(4));
        dag.add_edge(base, st1);
        dag.add_edge(base, st2);
        dag.add_edge(st1, st3);
        dag.add_edge(st1, st4);

        let eu_dag = ExecutionUnitDag::build_from_st_dag(&dag, |_| Some(RefreshMode::Differential));
        let levels = eu_dag.topological_levels().unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 2); // st1 and st2 units at level 0
        assert_eq!(levels[1].len(), 2); // st3 and st4 units at level 1
    }

    // ── C2-2: Topo sort caching tests ─────────────────────────────────

    #[test]
    fn test_topo_cache_populated_after_first_call() {
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));

        // Cache should be empty initially.
        assert!(dag.cached_topo.borrow().is_none());

        // First call populates the cache.
        let order1 = dag.topological_order().unwrap();
        assert!(dag.cached_topo.borrow().is_some());

        // Second call returns same result (from cache).
        let order2 = dag.topological_order().unwrap();
        assert_eq!(order1, order2);
    }

    #[test]
    fn test_topo_cache_invalidated_on_add_node() {
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));

        let _ = dag.topological_order().unwrap();
        assert!(dag.cached_topo.borrow().is_some());

        // Adding a node invalidates the cache.
        dag.add_st_node(make_dag_node(2));
        assert!(dag.cached_topo.borrow().is_none());
    }

    #[test]
    fn test_topo_cache_invalidated_on_add_edge() {
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));

        let _ = dag.topological_order().unwrap();
        assert!(dag.cached_topo.borrow().is_some());

        // Adding an edge invalidates the cache.
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        assert!(dag.cached_topo.borrow().is_none());
    }

    // ── G-8: Remove ST node tests ────────────────────────────────────

    #[test]
    fn test_remove_st_node_basic() {
        // base → st1 → st2
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        // Before removal: st1 and st2 exist.
        let order = dag.topological_order().unwrap();
        assert_eq!(order.len(), 2);

        // Remove st1.
        dag.remove_st_node(1);

        // st1 should be gone; st2 remains but with no upstream ST dependency.
        let order = dag.topological_order().unwrap();
        assert_eq!(order, vec![NodeId::StreamTable(2)]);
        assert!(dag.get_upstream(NodeId::StreamTable(2)).is_empty());
    }

    #[test]
    fn test_remove_st_node_nonexistent_is_noop() {
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));

        // Removing a non-existent node is a no-op.
        dag.remove_st_node(999);

        let order = dag.topological_order().unwrap();
        assert_eq!(order, vec![NodeId::StreamTable(1)]);
    }

    #[test]
    fn test_remove_st_node_cleans_reverse_edges() {
        // base → st1 → st2
        //            → st3
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_st_node(make_dag_node(3));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));

        dag.remove_st_node(1);

        // st2 and st3 should have no upstream ST dependencies.
        assert!(dag.get_upstream(NodeId::StreamTable(2)).is_empty());
        assert!(dag.get_upstream(NodeId::StreamTable(3)).is_empty());
        // Downstream of base table should be empty (st1 was removed).
        assert!(dag.get_downstream(NodeId::BaseTable(1)).is_empty());
    }

    #[test]
    fn test_remove_and_readd_st_node() {
        // base → st1 → st2
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        // Remove st1, then re-add with different deps.
        dag.remove_st_node(1);
        dag.add_st_node(make_dag_node(1));
        dag.add_edge(NodeId::BaseTable(2), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        // st1 should now depend on base(2) instead of base(1).
        let upstream = dag.get_upstream(NodeId::StreamTable(1));
        assert_eq!(upstream, vec![NodeId::BaseTable(2)]);

        // Chain st1 → st2 should still work.
        let order = dag.topological_order().unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], NodeId::StreamTable(1));
        assert_eq!(order[1], NodeId::StreamTable(2));
    }

    #[test]
    fn test_topo_cache_invalidated_on_remove() {
        let mut dag = StDag::new();
        dag.add_st_node(make_dag_node(1));
        dag.add_st_node(make_dag_node(2));
        dag.add_edge(NodeId::BaseTable(1), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        let _ = dag.topological_order().unwrap();
        assert!(dag.cached_topo.borrow().is_some());

        dag.remove_st_node(1);
        assert!(dag.cached_topo.borrow().is_none());
    }

    // ── DAG-4: Fused chain detection and fusion tests ───────────────────

    #[test]
    fn test_find_fusable_chains_simple_pair() {
        // base → st1 → st2: single-consumer chain, both DIFFERENTIAL
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // Should produce a single FusedChain with 2 members.
        assert_eq!(eu.unit_count(), 1);
        let unit = eu.units().next().unwrap();
        assert_eq!(unit.kind, ExecutionUnitKind::FusedChain);
        assert_eq!(unit.member_pgt_ids, vec![1, 2]);
    }

    #[test]
    fn test_find_fusable_chains_triple() {
        // base → st1 → st2 → st3: three-element chain
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        assert_eq!(eu.unit_count(), 1);
        let unit = eu.units().next().unwrap();
        assert_eq!(unit.kind, ExecutionUnitKind::FusedChain);
        assert_eq!(unit.member_pgt_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_find_fusable_chains_fan_out_not_eligible() {
        // base → st1 → st2
        //              → st3
        // st1 has two downstreams — not eligible for fusion.
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // All should remain singletons.
        assert_eq!(eu.unit_count(), 3);
        for unit in eu.units() {
            assert_eq!(unit.kind, ExecutionUnitKind::Singleton);
        }
    }

    #[test]
    fn test_find_fusable_chains_non_differential_not_eligible() {
        // base → st1 → st2: st1 is FULL mode — not eligible.
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, |id| {
            if id == 1 {
                Some(RefreshMode::Full)
            } else {
                Some(RefreshMode::Differential)
            }
        });

        assert_eq!(eu.unit_count(), 2);
        for unit in eu.units() {
            assert_eq!(unit.kind, ExecutionUnitKind::Singleton);
        }
    }

    #[test]
    fn test_find_fusable_chains_independent_singletons() {
        // base1 → st1, base2 → st2: no inter-ST dependency
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::BaseTable(200), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        assert_eq!(eu.unit_count(), 2);
        for unit in eu.units() {
            assert_eq!(unit.kind, ExecutionUnitKind::Singleton);
        }
    }

    #[test]
    fn test_fused_chain_preserves_external_edges() {
        // base → st1 → st2 → st3
        // st3 depends on base → st_ext also
        // After fusing st1→st2, the fused unit should have downstream to st3.
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // Should produce a single FusedChain of all 3.
        assert_eq!(eu.unit_count(), 1);
        let unit = eu.units().next().unwrap();
        assert_eq!(unit.kind, ExecutionUnitKind::FusedChain);
        assert_eq!(unit.member_pgt_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_fused_chain_stable_key() {
        let unit = ExecutionUnit {
            id: ExecutionUnitId(1),
            kind: ExecutionUnitKind::FusedChain,
            member_pgt_ids: vec![10, 20, 30],
            root_pgt_id: 10,
            label: "test".to_string(),
        };
        assert_eq!(unit.stable_key(), "fc:10,20,30");
    }

    #[test]
    fn test_fused_chain_kind_as_str() {
        assert_eq!(ExecutionUnitKind::FusedChain.as_str(), "fused_chain");
        assert_eq!(format!("{}", ExecutionUnitKind::FusedChain), "fused_chain");
    }

    #[test]
    fn test_is_calculated_unit_helper() {
        let unit = ExecutionUnit {
            id: ExecutionUnitId(1),
            kind: ExecutionUnitKind::Singleton,
            member_pgt_ids: vec![1],
            root_pgt_id: 1,
            label: "test".to_string(),
        };
        assert!(is_calculated_unit(&unit, &all_differential));
        assert!(!is_calculated_unit(&unit, &|_| Some(RefreshMode::Full)));
        assert!(!is_calculated_unit(&unit, &|_| None));
    }

    #[test]
    fn test_fused_chain_summary_includes_count() {
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);
        let summary = eu.summary();
        assert!(
            summary.contains("1 fused chains"),
            "summary should mention fused chains: {}",
            summary,
        );
    }

    #[test]
    fn test_fused_chain_topological_order() {
        // base → st1 → st2 → st3: fused into one unit
        let mut dag = StDag::new();
        dag.add_st_node(make_st_node(1, "public.st1"));
        dag.add_st_node(make_st_node(2, "public.st2"));
        dag.add_st_node(make_st_node(3, "public.st3"));
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        let eu = ExecutionUnitDag::build_from_st_dag(&dag, all_differential);

        // Topological order should succeed (no cycles).
        let order = eu.topological_order().unwrap();
        assert_eq!(order.len(), 1);
    }

    // ── T-2: Proptest — DAG cycle detection and topological order invariants ──

    #[cfg(test)]
    mod proptest_dag {
        use super::*;
        use proptest::prelude::*;

        /// Build an acyclic chain DAG: ST(1) → ST(2) → … → ST(n).
        fn build_chain_dag(n: i64) -> StDag {
            let mut dag = StDag::new();
            for id in 1..=n {
                dag.add_st_node(make_st(id, &format!("st{id}")));
            }
            // Chain: base → 1 → 2 → … → n
            dag.add_edge(NodeId::BaseTable(0), NodeId::StreamTable(1));
            for id in 1..n {
                dag.add_edge(NodeId::StreamTable(id), NodeId::StreamTable(id + 1));
            }
            dag
        }

        /// Build a DAG with a back-edge from ST(n) to ST(1), creating a cycle.
        fn build_cycle_dag(n: i64) -> StDag {
            let mut dag = build_chain_dag(n);
            dag.add_edge(NodeId::StreamTable(n), NodeId::StreamTable(1));
            dag
        }

        proptest! {
            /// An acyclic chain DAG must never trigger cycle detection.
            #[test]
            fn prop_acyclic_chain_detect_cycles_ok(n in 1i64..=20i64) {
                let dag = build_chain_dag(n);
                prop_assert!(
                    dag.detect_cycles().is_ok(),
                    "acyclic chain of length {n} should pass detect_cycles"
                );
            }

            /// A chain with a back-edge must be detected as cyclic.
            #[test]
            fn prop_chain_with_back_edge_is_cyclic(n in 2i64..=20i64) {
                let dag = build_cycle_dag(n);
                prop_assert!(
                    dag.detect_cycles().is_err(),
                    "chain of length {n} with back-edge should fail detect_cycles"
                );
            }

            /// Topological order of an acyclic chain preserves the dependency order.
            ///
            /// In a chain 1 → 2 → … → n (with base → 1), the topological order
            /// must place each ST before its downstream successor.
            #[test]
            fn prop_topological_order_respects_edges(n in 2i64..=15i64) {
                let dag = build_chain_dag(n);
                let order = dag.topological_order().expect("acyclic chain should have topo order");

                // Build a position map: NodeId → index in topological order.
                let pos: std::collections::HashMap<NodeId, usize> = order
                    .iter()
                    .enumerate()
                    .map(|(i, node)| (*node, i))
                    .collect();

                // For each ST edge (id → id+1), verify id comes before id+1.
                for id in 1..n {
                    let upstream = NodeId::StreamTable(id);
                    let downstream = NodeId::StreamTable(id + 1);
                    let up_pos = pos.get(&upstream).copied().unwrap_or(usize::MAX);
                    let dn_pos = pos.get(&downstream).copied().unwrap_or(usize::MAX);
                    prop_assert!(
                        up_pos < dn_pos,
                        "ST({id}) must appear before ST({}) in topo order",
                        id + 1
                    );
                }
            }

            /// Adding any single back-edge to an acyclic DAG makes it cyclic.
            #[test]
            fn prop_single_back_edge_always_creates_cycle(
                n in 2i64..=10i64,
                back in 2i64..=10i64,
            ) {
                // Clamp back to [2, n] so the back-edge is valid.
                let back = (back % (n - 1) + 2).min(n);
                // Build acyclic chain, then add ST(back) → ST(back - 1).
                let mut dag = build_chain_dag(n);
                dag.add_edge(NodeId::StreamTable(back), NodeId::StreamTable(back - 1));
                prop_assert!(
                    dag.detect_cycles().is_err(),
                    "adding back-edge ST({back}) → ST({}) should create a cycle",
                    back - 1
                );
            }
        }
    }
}
