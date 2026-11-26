NDB — Distributed NoSQL Search Database
Product Requirements, Software Architecture and Technical Development Specification
Document version: 1.0
Status: Initial engineering specification
Implementation language: Rust
Target operating systems: Ubuntu Linux and Windows Server (production); macOS (development platform)
Initial deployment: 3-node cluster
Long-term scalability: 100+ servers
Primary objective: High-performance distributed document database with integrated full-text, fuzzy, semantic and hybrid search.

1. Executive Summary
NDB is a distributed NoSQL database written primarily in Rust, designed to combine document storage, transactional database capabilities and advanced search functionality in a single platform.

The database will use a custom binary document format and a page-oriented B+Tree storage engine. Search capabilities will be implemented through specialised inverted indexes, BM25 relevance ranking, fuzzy matching and HNSW vector indexes.

The system will provide MongoDB-inspired document queries and aggregation pipelines through REST and gRPC interfaces.

The database will initially operate as a three-node replicated cluster, with an architecture designed to support more than 100 servers.

The primary design objectives are:

High-performance document storage and retrieval.
Low-latency full-text and hybrid search.
High-throughput document indexing.
Strong consistency for committed document transactions.
Search visibility within five seconds of document commitment.
Efficient memory utilisation.
Horizontal scalability.
Enterprise-grade security and multi-tenancy.
Cross-platform operation on Linux and Windows in production, with macOS supported as a development platform.
1.1 Capacity and performance objectives
Metric

Requirement

Raw document capacity

10 TB

Document count

Billions

Average document size

10 KB

Concurrent users

100+

Initial cluster

3 nodes

Maximum planned cluster

100+ nodes

Replication factor

3

Search latency

P95 below 50 ms

Search indexing visibility

Within 5 seconds

CPU minimum

8 cores per node

RAM minimum

8 GB per node

Recommended RAM

64–128 GB per node

Storage

NVMe SSD recommended

Operating systems

Ubuntu and Windows Server (production); macOS (development)

Database consistency

Strong

Search consistency

Near-real-time

API

REST and gRPC

Performance targets must be validated against explicitly defined benchmark workloads. They are engineering objectives rather than guaranteed performance across all query types.

1.2 Capacity interpretation
The 10 TB requirement represents raw document payload capacity.

It does not include:

Primary and secondary indexes.
Full-text indexes.
Vector indexes.
Replicated data.
WAL and recovery files.
Temporary storage.
Backups.
The physical storage requirement must therefore be calculated separately.

For example, with three replicas, 10 TB of raw document payload alone requires approximately 30 TB of aggregate document storage, before indexing overhead.

2. Product Scope
2.1 Included functionality
Document database
Database management.
Collection management.
Flexible document schemas.
Binary document storage.
CRUD operations.
Primary and secondary indexes.
Nested objects.
Arrays.
Data types and schema validation.
Multi-document transactions.
MVCC.
Document versioning.
Search engine
Full-text search.
BM25 ranking.
Field weighting.
Phrase search.
Fuzzy search.
Semantic search.
Vector similarity.
Hybrid search.
Structured filtering.
Sorting.
Pagination.
Faceted search.
Highlighting.
Autocomplete.
Search result scoring.
Distributed database
Cluster membership.
Sharding.
Replication.
Leader election.
Strong consistency.
Failover.
Automatic shard placement.
Data rebalancing.
Distributed query execution.
Cluster snapshots.
Node recovery.
Enterprise capabilities
Multiple databases.
Multiple tenants.
RBAC.
Database and collection permissions.
Field-level permissions.
TLS.
Encryption at rest.
Audit logging.
Backup and restore.
Monitoring.
Administrative APIs.
2.2 Out of scope for initial releases
The following should not be included in the first implementation:

MongoDB wire-protocol compatibility.
SQL compatibility.
Distributed SQL-style joins.
Cross-region active-active replication.
Automatic multi-region conflict resolution.
Built-in general-purpose machine learning inference.
Automatic schema inference across all documents.
Distributed transactions spanning arbitrary shard groups.
These can be considered after the core database is stable.

3. System Architecture
3.1 Architectural principles
The architecture must follow these principles:

Storage and search must be independent subsystems.
Document storage is the authoritative source of data.
Search indexes are derived, rebuildable structures.
All persisted formats must be versioned.
Distributed operations must have explicit consistency guarantees.
The storage engine must not depend on network interfaces.
Query execution must be independent of the physical storage implementation.
All background operations must support backpressure.
Memory consumption must be configurable and bounded.
The system must be observable at every architectural layer.
3.2 Logical architecture
The system will consist of the following components:

Client layer
REST API.
gRPC API.
Rust client SDK.
Authentication and session handling.
Query layer
Request validation.
Query parser.
Abstract syntax tree.
Query planner.
Query optimiser.
Query executor.
Result formatting.
Database engine
Document manager.
Collection manager.
Schema manager.
Transaction manager.
Index manager.
Search engine
Tokenisation.
Text analysis.
Inverted indexes.
BM25 ranking.
Fuzzy search.
Vector indexing.
Hybrid search.
Search result fusion.
Storage engine
Page manager.
Buffer pool.
B+Tree.
Record manager.
WAL.
Checkpoint manager.
Recovery manager.
Distributed engine
Cluster manager.
Raft replication.
Shard manager.
Query coordinator.
Routing.
Rebalancing.
Distributed snapshots.
Security engine
Authentication.
Authorisation.
Encryption.
Audit logging.
Key management.
4. Distributed Architecture
4.1 Initial topology
The initial deployment will consist of three database nodes.

Each shard will have three replicas.

Each replica group will have:

One Raft leader.
Two Raft followers.
A replicated transaction log.
Local persistent storage.
Local B+Tree indexes.
Local search indexes.
Local vector indexes.
A shard leader is responsible for accepting writes and coordinating committed transactions.

Writes must be replicated to a majority before being acknowledged.

A three-replica group requires two replicas, including the leader, to commit a transaction.

A failure of one node must not result in the loss of committed data or prevent the remaining majority from operating.

A failure of two nodes must prevent writes to the affected group until a majority is restored.

4.2 Raft implementation
Use an established Rust consensus implementation initially, such as OpenRaft, while retaining ownership of the database replication and storage integration.

The database must implement its own:

Raft state-machine integration.
Log persistence adapter.
Snapshot generation.
Snapshot restoration.
Membership management.
Shard-level replication.
Transaction application.
Recovery coordination.
Consensus must not be implemented independently for every database operation.

The Raft replicated log is the authoritative ordering mechanism for replicated transactions.

The local storage WAL supports durable page changes and recovery. It must be coordinated with the Raft commit index and transaction identifiers.

4.3 Sharding
The database must support virtual shards.

Collections will be divided into logical partitions that can be distributed independently of the physical server count.

Recommended initial configuration:

256 virtual shards per large collection.
Configurable shard count.
Three replicas per shard.
Consistent hash or range-based partition assignment.
Automatic placement and rebalancing.
The initial number of virtual shards is a starting configuration and should be benchmarked.

Shard partitioning strategies
Support:

Hash partitioning.
Range partitioning.
Tenant-based partitioning.
Hash partitioning is the default for evenly distributed document identifiers.

Range partitioning is useful for ordered keys and time-oriented workloads.

Tenant-based partitioning provides an option for tenant isolation and data locality.

4.4 Cluster management
The cluster manager must maintain:

Cluster identifier.
Node identifiers.
Node addresses.
Node health.
Shard membership.
Leader information.
Replica placement.
Cluster configuration version.
Cluster metadata version.
Cluster metadata must itself be replicated and recoverable.

The system must support adding and removing nodes without requiring a complete cluster restart.

4.5 Rebalancing
Rebalancing must be incremental.

Requirements:

Move shards between nodes.
Preserve strong consistency.
Avoid duplicate document visibility.
Maintain replica count.
Support interrupted transfers.
Resume incomplete transfers.
Throttle data movement.
Prioritise operational stability over migration speed.
A shard transfer must have explicit ownership transitions. A shard must not accept conflicting writes from two leaders.

5. Storage Engine
5.1 General design
Implement a custom page-oriented B+Tree storage engine.

The storage engine must provide:

Durable storage.
Efficient point lookups.
Ordered scans.
Range scans.
Secondary indexes.
Concurrent access.
Crash recovery.
MVCC integration.
The initial implementation should favour correctness and recoverability over premature low-level optimisation.

5.2 Page manager
The page manager is responsible for allocating, reading, writing and releasing pages.

Initial configuration:

Property

Proposed value

Page size

16 KB

Page identifier

64-bit

Page format

Versioned binary

File access

Random access

Page allocation

Extent-based

Buffer management

Configurable buffer pool

Durability

WAL-backed

Benchmark page sizes of 8 KB, 16 KB and 32 KB before finalising the production default.

Page header
Each page must contain:

Page ID.
Page type.
Page format version.
Generation number.
Checksum.
Flags.
LSN.
Free-space information.
Checksums must detect page corruption.

Page generations should protect against stale references when pages are reused.

5.3 B+Tree implementation
Implement:

Internal nodes.
Leaf nodes.
Node splitting.
Node merging.
Redistribution.
Root management.
Cursor navigation.
Ordered iterators.
Range scans.
Composite keys.
Prefix operations.
Leaf nodes must maintain ordered keys.

The implementation must support concurrent readers and writers.

Use a latch-coupling or another carefully validated concurrency strategy.

Do not introduce lock-free B+Tree operations until the basic implementation is correct and benchmarked.

5.4 Buffer pool
Responsibilities:

Load pages from storage.
Cache pages in RAM.
Track pinned pages.
Track dirty pages.
Evict unused pages.
Flush pages safely.
Coordinate concurrent access.
Initial eviction candidates:

Clock replacement.
Segmented LRU.
Memory limits must be configurable globally and per subsystem.

The buffer pool must never allow unbounded memory growth.

5.5 WAL
Implement a write-ahead logging subsystem.

Requirements:

Append-only records.
Checksums.
Sequence numbers.
Transaction identifiers.
Commit markers.
Recovery scanning.
Log rotation.
Checkpoint support.
Torn-write detection.
Durable flush operations.
All modified pages must be recoverable from committed log records.

The database must never acknowledge a committed transaction before its required durability conditions are satisfied.

5.6 Checkpointing
The checkpoint manager must:

Identify the checkpoint boundary.
Flush eligible dirty pages.
Persist transaction metadata.
Persist recovery metadata.
Record the completed checkpoint.
Safely retire obsolete WAL segments.
Checkpointing must not block all normal database operations.

5.7 MVCC
Use multi-version concurrency control.

Each document version must have:

Transaction identifier.
Commit timestamp or sequence.
Version identifier.
Visibility information.
Previous-version reference where necessary.
Requirements:

Snapshot reads.
Concurrent readers.
Concurrent writers.
Repeatable reads.
Transaction isolation documentation.
Safe version garbage collection.
The initial target isolation level is snapshot isolation.

Serializable isolation can be added later if required.

Long-running transactions must not prevent unlimited version retention. Introduce configurable transaction timeouts and monitoring of old snapshots.

6. Binary Document Format
6.1 Design objectives
The binary document format must provide:

Compact storage.
Efficient parsing.
Partial field retrieval.
Nested object support.
Array support.
Schema flexibility.
Cross-platform portability.
Version compatibility.
Forward-compatible evolution.
The format must not depend on Rust compiler struct layout.

6.2 Document structure
Each document will contain:

Document header
Format version.
Document identifier.
Collection identifier.
Schema version.
Document version.
Payload size.
Flags.
Field directory
Each field entry contains:

Numeric field identifier.
Data type.
Offset.
Length.
Optional encoding flags.
Payload
Contains the encoded values.

6.3 Supported data types
Initial types:

Null.
Boolean.
Signed integer.
Unsigned integer.
Float.
Decimal.
String.
Binary.
Date/time.
Object.
Array.
UUID.
Vector.
Vector values should have a distinct typed representation.

6.4 Field dictionaries
Each collection maintains a field dictionary.

Example:

Field ID

Field name

Type

1

name

String

2

category

String

3

price

Decimal

4

active

Boolean

5

embedding

Vector

Field identifiers must remain stable for the lifetime of their schema versions.

Schema evolution must not silently reinterpret existing field identifiers.

6.5 Compression
Compression should be configurable.

Candidates:

LZ4 for speed.
Zstandard for compression efficiency.
Compression should be performed at appropriate storage-block or document-group boundaries.

Small documents should not automatically be compressed if the overhead exceeds the benefit.

7. Transaction Processing
7.1 Transaction lifecycle
A transaction must support:

Begin.
Read.
Insert.
Update.
Delete.
Commit.
Rollback.
7.2 Write processing
For a single-shard write:

Authenticate the request.
Validate permissions.
Validate document and schema.
Identify the target shard.
Forward to the shard leader.
Create transaction metadata.
Append transaction operations to the replicated log.
Replicate to a majority.
Commit through Raft.
Apply the transaction to the storage engine.
Publish a durable indexing event.
Acknowledge the committed write.
The implementation must ensure that the document is not visible as committed before its transaction has been committed by the replicated state machine.

7.3 Indexing consistency
Document transactions and search indexing have different visibility guarantees.

Document reads must observe committed versions according to the selected transaction isolation level.

Search indexes may lag behind the document store by up to five seconds.

The indexing pipeline must be:

Durable.
Ordered per document.
Idempotent.
Recoverable.
Observable.
Backpressure-aware.
Each indexing operation must include:

Document ID.
Collection ID.
Document version.
Commit sequence.
Operation type.
Changed fields.
Required index versions.
7.4 Search visibility
Maintain an indexing watermark per shard and index generation.

A query requiring visibility through a particular committed sequence must either:

Wait until the index reaches the requested sequence.
Return a timeout.
Use a direct document-store fallback where appropriate.
The database must not silently claim that a document is searchable when its required index updates have not been applied.

8. Indexing Architecture
8.1 Index manager
The index manager coordinates:

Primary indexes.
Secondary indexes.
Full-text indexes.
Vector indexes.
Bitmap indexes.
Index creation.
Index deletion.
Rebuilding.
Version management.
Index validation.
All indexes must be associated with collection metadata and index definitions.

8.2 Primary index
Every document must have a unique primary identifier.

The primary index maps:

Document ID → Record location.

It must support efficient point retrieval.

8.3 Secondary indexes
Support:

Single-field indexes.
Composite indexes.
Unique indexes.
Sparse indexes.
Partial indexes.
Ordered indexes.
Range indexes.
The query planner must be able to identify whether an index can satisfy a query predicate.

8.4 Bitmap indexes
Support bitmap indexes for suitable low-cardinality fields.

Examples:

Status.
Category.
Tenant.
Boolean flags.
Document type.
Bitmap indexes should be optional and configurable.

9. Full-Text Search Engine
9.1 Inverted index
Implement a custom inverted index.

The structure consists of:

Term dictionary.
Posting lists.
Document identifiers.
Term frequencies.
Field identifiers.
Position information.
Document lengths.
Collection statistics.
9.2 Text analysis
The analysis pipeline must support:

Unicode normalisation.
Tokenisation.
Case folding.
Stop-word filtering.
Stemming.
Optional language-specific analysis.
Synonym expansion.
Text analysis must be configurable per field.

The analysis configuration must be versioned. Changing an analyser should trigger an index migration or rebuild rather than silently mixing incompatible tokenisation rules.

9.3 BM25
Implement BM25 ranking.

Default starting parameters:

k1 = 1.2.
b = 0.75.
Both must be configurable.

Support field weighting.

Example:

Field

Weight

Title

3.0

Description

1.5

Content

1.0

Tags

2.0

The implementation must maintain:

Document frequency.
Term frequency.
Average field length.
Collection document count.
Field statistics.
For distributed search, use versioned global collection statistics so that local shard scores can be compared meaningfully.

9.4 Posting compression
Implement and benchmark:

Delta encoding.
Variable-byte encoding.
Block-based integer compression.
Skip information.
Block-max scoring metadata.
Use WAND and Block-Max WAND where appropriate to improve top-K search performance.

9.5 Phrase search
Posting lists should include term positions.

Phrase search must preserve token ordering and support configurable slop.

9.6 Fuzzy search
Implement approximate term matching using a term dictionary.

Evaluate:

Levenshtein distance.
Symmetric Delete indexing.
Trie-based lookup.
Bounded edit-distance algorithms.
Fuzzy matching must have configurable edit-distance limits and expansion limits.

Avoid unbounded dictionary expansion.

10. Vector Search Engine
10.1 Requirements
Support:

Configurable dimensions.
Multiple vector fields.
Multiple vector indexes per collection.
Cosine similarity.
Dot product.
Euclidean distance.
Approximate nearest-neighbour search.
Exact nearest-neighbour search for suitable datasets.
Filtered vector search.
Vector updates and deletions.
10.2 HNSW
Implement HNSW as the initial vector-index algorithm.

Configuration:

M.
efConstruction.
efSearch.
Maximum vector dimensions.
Distance metric.
The parameters must be configurable per index.

10.3 Vector storage
Support:

Float32.
Float16.
Quantised representations.
Start with Float32 as the reference implementation.

Introduce quantisation after correctness tests and quality benchmarks.

The vector engine must support disk-backed storage and configurable memory budgets.

10.4 Embedding generation
Applications may supply embeddings.

The database may also integrate external embedding providers through configurable adapters.

Model inference must be isolated from the core transaction and storage engine.

An embedding provider must never block the Raft commit process.

Store embedding metadata:

Provider.
Model name.
Model version.
Dimensions.
Similarity metric.
Generation timestamp.
Changing embedding models must not silently mix incompatible vector spaces.

11. Hybrid Search
11.1 Supported search modes
Full-text only.
Vector only.
Fuzzy only.
Full-text with filters.
Vector with filters.
Full-text plus vector.
Full-text plus vector plus filters.
11.2 Ranking fusion
Implement Reciprocal Rank Fusion initially.

Allow configurable RRF parameters.

Future options may include weighted score fusion and learned ranking models.

11.3 Hybrid execution
The hybrid query executor must:

Validate the query.
Identify applicable indexes.
Apply tenant and security filters.
Execute full-text retrieval.
Execute vector retrieval.
Merge candidates.
Calculate fusion scores.
Apply final ranking.
Retrieve documents.
Apply projections.
Return results.
Search result candidates should be represented using compact document identifiers and scores.

Do not retrieve full document payloads until the final result set is known.

12. Query Language
12.1 Design
Use a MongoDB-inspired JSON query language.

Do not attempt complete MongoDB compatibility.

The query language must have a versioned grammar and a documented operator catalogue.

12.2 Comparison operators
$eq
$ne
$gt
$gte
$lt
$lte
$in
$nin
12.3 Logical operators
$and
$or
$not
$nor
12.4 Array operators
$all
$elemMatch
$size
12.5 Existence and type operators
$exists
$type
12.6 Text operators
$text
$search
$phrase
$fuzzy
12.7 Vector operators
$vectorSearch
$vectorFilter
12.8 Query execution
Implement a multi-stage execution pipeline:

Parse JSON.
Build AST.
Validate syntax and types.
Validate authorisation.
Resolve fields.
Identify available indexes.
Estimate selectivity.
Generate execution plan.
Execute operations.
Format results.
The planner must expose an EXPLAIN operation so developers can inspect index selection and execution plans.

12.9 Aggregation framework
Support an aggregation pipeline.

Initial stages:

$match
$project
$group
$sort
$limit
$skip
$count
$unwind
$addFields
Initial expressions:

$sum
$avg
$min
$max
$first
$last
$concat
$toString
$ifNull
The execution engine must support bounded memory consumption and disk spilling for large aggregation workloads.

13. Query Optimisation
The query optimiser must be independent of the query parser.

Its responsibilities include:

Index selection.
Predicate pushdown.
Filter ordering.
Projection pushdown.
Top-K optimisation.
Early termination.
Search candidate reduction.
Query plan caching.
The optimiser should use collection statistics and index statistics.

Every query must have:

Maximum execution time.
Maximum memory allocation.
Maximum result size.
Cancellation support.
Deep pagination should use cursor-based pagination rather than large offset scans.

14. Distributed Query Execution
The distributed coordinator is responsible for:

Query routing.
Shard discovery.
Parallel execution.
Timeout management.
Result merging.
Global sorting.
Global top-K.
Distributed aggregations.
Search statistics.
Each shard must execute as much work locally as possible.

The coordinator should receive compact candidate records rather than complete documents.

Distributed queries must preserve the requested consistency semantics.

Search requests may use an explicitly selected indexed version or watermark.

The query response should include indexing freshness metadata when relevant.

15. REST API
Implement REST using Axum.

All endpoints must use versioned paths.

Base path:

/api/v1

15.1 Database endpoints
Method

Endpoint

Description

GET

/databases

List databases

POST

/databases

Create database

GET

/databases/{id}

Get database

DELETE

/databases/{id}

Delete database

15.2 Collection endpoints
Method

Endpoint

Description

GET

/databases/{db}/collections

List collections

POST

/databases/{db}/collections

Create collection

GET

/collections/{id}

Collection details

DELETE

/collections/{id}

Delete collection

15.3 Document endpoints
Method

Endpoint

Description

POST

/collections/{id}/documents

Insert document

GET

/collections/{id}/documents/{doc}

Retrieve document

PUT

/collections/{id}/documents/{doc}

Replace document

PATCH

/collections/{id}/documents/{doc}

Update document

DELETE

/collections/{id}/documents/{doc}

Delete document

POST

/collections/{id}/documents/bulk

Bulk operations

15.4 Search endpoints
Method

Endpoint

Description

POST

/collections/{id}/search

Full-text search

POST

/collections/{id}/vector-search

Vector search

POST

/collections/{id}/hybrid-search

Hybrid search

POST

/collections/{id}/aggregate

Aggregation

POST

/collections/{id}/explain

Query explanation

15.5 Index endpoints
Create index.
Delete index.
List indexes.
Get index status.
Rebuild index.
Validate index.
15.6 Cluster endpoints
Cluster status.
Node status.
Shard status.
Replica status.
Membership management.
Rebalancing status.
Administrative endpoints must require elevated permissions.

16. gRPC API
Implement gRPC using Tonic and Protocol Buffers.

Services:

DatabaseService.
CollectionService.
DocumentService.
SearchService.
IndexService.
TransactionService.
ClusterService.
AdministrationService.
The gRPC API must support streaming for:

Bulk document insertion.
Bulk retrieval.
Large search results.
Backup and restore transfers.
Administrative status updates.
Protocol definitions must be versioned.

17. Multi-Tenancy and Security
17.1 Tenant model
Support:

Cluster → Tenants → Databases → Collections → Documents.

Tenants must have independent permissions and configurable resource limits.

Tenant isolation must be enforced in both document queries and search-index access.

17.2 RBAC
Implement:

Users.
Roles.
Permissions.
Role assignments.
Database-level access.
Collection-level access.
Operation-level permissions.
Field-level restrictions.
Authorisation must happen before query execution and must be enforced throughout aggregation and search pipelines.

17.3 Authentication
Support:

Username/password.
API keys.
Token-based authentication.
Mutual TLS.
Service-to-service credentials.
17.4 Encryption
Implement:

TLS 1.3.
AES-256-GCM or ChaCha20-Poly1305.
Envelope encryption.
Key rotation.
Encrypted backups.
Encrypted WAL and storage files.
Use an external key-management integration where appropriate.

17.5 Audit logging
Audit:

Authentication.
Failed access attempts.
User creation.
Role modifications.
Database modifications.
Collection modifications.
Index changes.
Configuration changes.
Administrative actions.
Backup and restore.
Audit records must be tamper-resistant.

18. Backup and Recovery
The database must support:

Full backups.
Incremental backups.
Point-in-time recovery.
Snapshot restoration.
Shard-level restoration.
Collection-level restoration where feasible.
Encrypted backup files.
Backups must include:

Document data.
Transaction metadata.
Schema definitions.
Index definitions.
Required recovery metadata.
Cluster metadata.
Search indexes may be restored from backup or rebuilt from document data and indexing metadata.

Backup restoration must be tested regularly.

A backup is not considered valid until it has passed a restoration test.

19. Rust Workspace and Crate Architecture
Use a Cargo workspace with clearly separated crates.

ndb/
│
├── Cargo.toml
│
├── crates/
│   ├── ndb-common/
│   ├── ndb-config/
│   ├── ndb-document/
│   ├── ndb-storage/
│   ├── ndb-page/
│   ├── ndb-buffer/
│   ├── ndb-btree/
│   ├── ndb-wal/
│   ├── ndb-transaction/
│   ├── ndb-mvcc/
│   ├── ndb-index/
│   ├── ndb-bitmap/
│   ├── ndb-tokenizer/
│   ├── ndb-analyzer/
│   ├── ndb-inverted/
│   ├── ndb-postings/
│   ├── ndb-bm25/
│   ├── ndb-fuzzy/
│   ├── ndb-vector/
│   ├── ndb-hnsw/
│   ├── ndb-hybrid/
│   ├── ndb-query-ast/
│   ├── ndb-query-parser/
│   ├── ndb-query-planner/
│   ├── ndb-query-optimizer/
│   ├── ndb-query-executor/
│   ├── ndb-aggregation/
│   ├── ndb-cluster/
│   ├── ndb-sharding/
│   ├── ndb-replication/
│   ├── ndb-distributed-query/
│   ├── ndb-security/
│   ├── ndb-audit/
│   ├── ndb-rest/
│   ├── ndb-grpc/
│   ├── ndb-client/
│   ├── ndb-server/
│   └── ndb-admin/
│
├── proto/
├── tests/
├── benchmarks/
├── examples/
├── docs/
└── tools/
19.1 Crate responsibilities
ndb-common
Shared identifiers, error types, result types, common traits and configuration primitives.

ndb-document
Binary document encoding, decoding, field dictionaries and document validation.

ndb-storage
Physical file management and persistent storage.

ndb-page
Page structures, page allocation and page checksums.

ndb-buffer
Buffer pool and memory management.

ndb-btree
Primary and secondary B+Tree implementations.

ndb-wal
Write-ahead logging and recovery records.

ndb-transaction
Transaction lifecycle and commit coordination.

ndb-mvcc
Version management and snapshot visibility.

ndb-search crates
Text analysis, inverted indexes, BM25, fuzzy search and index segment management.

ndb-vector
Vector storage, similarity calculations, HNSW and quantisation.

ndb-query crates
Query parsing, AST, planning, optimisation and execution.

ndb-cluster
Cluster membership, node management and shard placement.

ndb-replication
Raft integration and replicated state-machine application.

ndb-distributed-query
Query routing, parallel execution and result merging.

ndb-security
Authentication, authorisation and encryption.

ndb-server
Application composition and server lifecycle.

19.2 Dependency rules
The dependency graph must be deliberately controlled.

Common must not depend on database engines.
Document must not depend on networking.
Storage must not depend on search.
B+Tree must not depend on the query engine.
Search must access documents through defined interfaces.
Query planning must not depend on a specific physical storage implementation.
Distributed routing must depend on storage and search abstractions.
REST and gRPC must depend on service interfaces rather than physical storage crates.
Circular dependencies are prohibited.

20. Core Rust Interfaces
The following interfaces are conceptual contracts. Their exact signatures should be refined during implementation.

20.1 Document storage
pub trait DocumentStore: Send + Sync {
    async fn insert(
        &self,
        collection: CollectionId,
        document: Document,
    ) -> Result<DocumentId>;

    async fn get(
        &self,
        collection: CollectionId,
        id: DocumentId,
    ) -> Result<Option<Document>>;

    async fn update(
        &self,
        collection: CollectionId,
        id: DocumentId,
        document: Document,
    ) -> Result<()>;

    async fn delete(
        &self,
        collection: CollectionId,
        id: DocumentId,
    ) -> Result<()>;
}
20.2 Search
pub trait SearchEngine: Send + Sync {
    async fn search(
        &self,
        request: SearchRequest,
    ) -> Result<SearchResponse>;
}
20.3 Index management
pub trait IndexManager: Send + Sync {
    async fn create_index(
        &self,
        definition: IndexDefinition,
    ) -> Result<IndexId>;

    async fn drop_index(
        &self,
        index: IndexId,
    ) -> Result<()>;

    async fn rebuild_index(
        &self,
        index: IndexId,
    ) -> Result<()>;
}
20.4 Transaction management
pub trait TransactionManager: Send + Sync {
    async fn begin(
        &self,
    ) -> Result<TransactionId>;

    async fn commit(
        &self,
        transaction: TransactionId,
    ) -> Result<CommitVersion>;

    async fn rollback(
        &self,
        transaction: TransactionId,
    ) -> Result<()>;
}
Important implementation rule: asynchronous interfaces should be introduced only where asynchronous work is genuinely required. Internal storage operations may use synchronous abstractions on dedicated blocking execution contexts.

Avoid introducing async overhead into every low-level page operation.

21. Technology and Dependency Policy
The database core must be custom, but supporting libraries should be selected carefully.

Area

Candidate

Async runtime

Tokio

REST

Axum

gRPC

Tonic

Serialization

Serde

Consensus

OpenRaft

Compression

Zstandard

Cryptography

RustCrypto

Metrics

Prometheus

Tracing

tracing

Testing

Proptest

Benchmarking

Criterion

Property testing

Proptest

Protocols

Protocol Buffers

No external database may be used as the authoritative storage engine.

No external search engine may be required for normal operation.

No external vector database may be required.

The system must remain self-contained apart from optional embedding services, monitoring systems and external key-management providers.

Before adopting any dependency, verify:

Maintenance status.
Licence.
Windows compatibility.
Linux compatibility.
macOS compatibility.
Security history.
Performance characteristics.
Long-term compatibility.
22. Testing Strategy
Testing is a first-class development requirement.

22.1 Unit tests
Every crate must have independent tests.

Important areas:

Binary encoding.
Binary decoding.
Page allocation.
B+Tree insertion.
B+Tree deletion.
B+Tree splitting.
B+Tree recovery.
WAL replay.
MVCC visibility.
Transaction rollback.
Posting-list decoding.
BM25 ranking.
Fuzzy matching.
Vector similarity.
HNSW traversal.
Query parsing.
Query optimisation.
22.2 Property-based testing
Use Proptest for:

B+Tree invariants.
Random document encoding.
Random transaction sequences.
WAL recovery.
Query parser correctness.
Index consistency.
Document version visibility.
22.3 Crash testing
Simulate process termination at every significant persistence boundary.

Test:

Before WAL append.
During WAL append.
After WAL persistence.
During replication.
After Raft commitment.
During page modification.
During checkpointing.
During index segment creation.
During index merging.
During snapshot restoration.
No acknowledged committed transaction may be lost.

22.4 Distributed testing
Test:

Node termination.
Network partitions.
Delayed network messages.
Duplicate messages.
Leader failure.
Follower failure.
Replica replacement.
Shard migration.
Cluster membership changes.
Recovery following prolonged downtime.
22.5 Security testing
Test:

Unauthorised database access.
Cross-tenant data leakage.
Field-level permission bypass.
Aggregation permission bypass.
Search permission bypass.
Token expiry.
TLS certificate validation.
Key rotation.
Audit integrity.
23. Benchmarking and Performance Acceptance
Create a dedicated benchmarking application.

It must be possible to run benchmarks against:

Individual storage crates.
The complete single-node engine.
The search engine.
The vector engine.
Distributed clusters.
23.1 Initial benchmark dataset
Start with:

1 million documents.
10 KB average document size.
Multiple indexed fields.
Text content.
Optional vectors.
Different field cardinalities.
Then expand to:

10 million.
100 million.
1 billion documents.
The 10 TB target must ultimately be tested using representative storage hardware and a realistic cluster configuration.

23.2 Initial performance targets
Workload

Target

Primary key lookup

P95 under 5 ms

Indexed filtering

P95 under 10 ms

Full-text search

P95 under 50 ms

Hybrid search

P95 under 50 ms

Vector search

P95 under 50 ms

Search indexing visibility

Within 5 seconds

Concurrent users

100+

Cluster failover

Under 30 seconds

Committed write durability

No acknowledged commit loss

Define separate cold-cache and warm-cache benchmarks.

Search performance must be measured during concurrent indexing.

Performance reports must include:

P50.
P95.
P99.
Throughput.
CPU utilisation.
Memory usage.
Disk utilisation.
Network utilisation.
Indexing lag.
Replication lag.
24. Monitoring and Observability
Implement structured logging and metrics from the beginning.

24.1 Storage metrics
Pages read.
Pages written.
Buffer pool hit ratio.
Buffer pool eviction rate.
Dirty pages.
WAL size.
Checkpoint duration.
Recovery duration.
24.2 Search metrics
Queries per second.
Query latency.
Posting lists scanned.
Candidate documents.
Documents scored.
Vector search latency.
Indexing throughput.
Indexing backlog.
Search visibility lag.
24.3 Distributed metrics
Raft commit latency.
Replication lag.
Leader elections.
Node health.
Shard availability.
Shard movement.
Network traffic.
Replica recovery.
24.4 Security metrics
Authentication failures.
Authorisation failures.
API key usage.
Certificate expiry.
Encryption errors.
Audit events.
Provide a health endpoint and Prometheus-compatible metrics endpoint.

25. Configuration
The server must be configurable through a versioned YAML configuration file.

Illustrative configuration:

node:
  id: node-001
  listen_address: 0.0.0.0
  rest_port: 8080
  grpc_port: 9090

cluster:
  name: ndb-cluster
  replication_factor: 3
  consensus: raft

storage:
  path: ./data
  page_size: 16384
  buffer_pool_size: 3GB
  wal_enabled: true
  checkpoint_interval_seconds: 60

transactions:
  isolation: snapshot
  timeout_seconds: 30

indexing:
  max_visibility_seconds: 5
  batch_size: 1000
  max_workers: 8
  durable_queue: true

search:
  default_limit: 20
  max_limit: 1000

  bm25:
    k1: 1.2
    b: 0.75

  fuzzy:
    enabled: true
    max_distance: 2

vector:
  default_algorithm: hnsw
  default_metric: cosine

security:
  authentication_enabled: true
  tls_enabled: true
  encryption_at_rest: true
  audit_enabled: true

logging:
  level: info
  structured: true

monitoring:
  metrics_enabled: true
Configuration validation must happen before the server starts.

Invalid configuration must produce clear errors rather than silently applying defaults.

26. Implementation Roadmap
The implementation should follow controlled engineering milestones.

The following estimates assume a small experienced Rust/database engineering team. They are indicative rather than contractual.

Phase 0 — Architecture and foundations
Estimated duration: 2–3 weeks

Deliverables:

Final architecture.
ADR framework.
Cargo workspace.
Error handling.
Configuration framework.
Logging.
Metrics.
CI build and test pipelines.
Cross-platform build verification.
Acceptance:

Clean builds on Ubuntu, Windows and macOS.
Workspace dependencies established.
Automated tests running.
Architecture documentation published.
Phase 1 — Binary document storage
Estimated duration: 4–6 weeks

Deliverables:

Binary document format.
Field dictionary.
Schema definitions.
Page manager.
File manager.
Page allocation.
Page checksums.
Acceptance:

Encode/decode documents.
Support nested documents.
Support arrays.
Support partial field access.
Validate binary format integrity.
Phase 2 — B+Tree engine
Estimated duration: 6–10 weeks

Deliverables:

B+Tree.
Page splitting.
Page merging.
Cursors.
Range scans.
Composite keys.
Buffer pool.
Concurrent access.
Acceptance:

Millions of test operations.
No B+Tree invariant violations.
Repeatable performance benchmarks.
Phase 3 — Transactions and recovery
Estimated duration: 6–8 weeks

Deliverables:

WAL.
Transaction manager.
MVCC.
Commit processing.
Rollback.
Checkpointing.
Crash recovery.
Acceptance:

Forced termination tests pass.
Committed transactions survive restart.
Uncommitted transactions are not exposed.
Concurrent readers and writers work correctly.
Phase 4 — Database functionality
Estimated duration: 4–6 weeks

Deliverables:

Database manager.
Collection manager.
Schema validation.
CRUD.
Primary indexes.
Secondary indexes.
Bulk insertion.
Acceptance:

Complete single-node document database.
Automated integration tests.
Document persistence verified.
Phase 5 — Full-text search
Estimated duration: 8–12 weeks

Deliverables:

Tokeniser.
Text analyser.
Inverted index.
Posting lists.
BM25.
Phrase search.
Fuzzy matching.
Segment management.
Index compaction.
Acceptance:

Search correctness tests.
Relevance evaluation.
Indexing throughput benchmarks.
Search latency benchmarks.
Phase 6 — Query engine
Estimated duration: 6–10 weeks

Deliverables:

MongoDB-style parser.
Query AST.
Validation.
Query planner.
Query optimiser.
Filters.
Sorting.
Projection.
Aggregations.
EXPLAIN.
Acceptance:

Query-language tests.
Correct index selection.
Query cancellation.
Bounded memory execution.
Phase 7 — Vector and hybrid search
Estimated duration: 8–12 weeks

Deliverables:

Vector storage.
Distance functions.
HNSW.
Vector updates.
Vector filtering.
Quantisation groundwork.
Hybrid search.
RRF.
Acceptance:

Vector recall benchmarks.
Hybrid ranking tests.
Memory profiling.
Search performance validation.
Phase 8 — API and SDK
Estimated duration: 4–6 weeks

Deliverables:

REST API.
gRPC API.
Rust SDK.
Authentication middleware.
Bulk operations.
Streaming.
API documentation.
Acceptance:

Integration tests.
API compatibility tests.
Client SDK examples.
Phase 9 — Distributed database
Estimated duration: 12–18 weeks

Deliverables:

Three-node cluster.
Raft integration.
Replicated transactions.
Shard routing.
Cluster metadata.
Leader election.
Failure recovery.
Distributed query execution.
Distributed search.
Acceptance:

Node failure testing.
Network partition testing.
Strong consistency tests.
Distributed search tests.
Recovery testing.
Phase 10 — Enterprise security
Estimated duration: 6–8 weeks

Deliverables:

Multi-tenancy.
RBAC.
Authentication.
TLS.
Encryption at rest.
Audit logging.
Key management.
Backup and restore.
Acceptance:

Security test suite.
Permission isolation.
Encrypted backup restoration.
Phase 11 — Production hardening
Estimated duration: 12–20 weeks

Deliverables:

Online index management.
Automatic rebalancing.
Backup automation.
Cluster administration.
Operational monitoring.
Ubuntu and Windows production validation.
Large dataset testing.
Performance optimisation.
Acceptance:

Production readiness review.
Full recovery tests.
Benchmark reports.
Operational documentation.
Indicative overall delivery
A small experienced team might deliver an early functional distributed database in approximately 12–18 months.

A production-grade system with the full feature set, extensive hardening and large-scale validation could require considerably longer.

These estimates are dependent on team size, implementation quality, testing coverage and the amount of parallel development.

27. Definition of Done
A feature is not complete simply because its code compiles.

Every feature must include:

Implementation.
Unit tests.
Integration tests where applicable.
Error handling.
Logging.
Metrics where appropriate.
Documentation.
Performance tests where applicable.
Security considerations.
Cross-platform validation where relevant.
Every storage and transaction change must include appropriate recovery tests.

Every distributed change must include failure testing.

Every query-engine change must include correctness and resource-limit tests.

28. Architecture Decision Records
Maintain ADRs for all significant engineering decisions.

Initial ADR list:

ADR

Decision

ADR-001

Rust as primary implementation language

ADR-002

Cargo workspace architecture

ADR-003

Custom binary document format

ADR-004

Page-oriented storage

ADR-005

B+Tree primary storage

ADR-006

WAL design

ADR-007

MVCC and transaction isolation

ADR-008

Raft-based replication

ADR-009

Virtual shard architecture

ADR-010

Three replicas per shard

ADR-011

Inverted index design

ADR-012

BM25 ranking

ADR-013

HNSW vector search

ADR-014

Hybrid search using RRF

ADR-015

MongoDB-inspired query language

ADR-016

Near-real-time search visibility

ADR-017

REST and gRPC interfaces

ADR-018

Multi-tenancy and RBAC

ADR-019

Encryption and key management

ADR-020

Cross-platform storage abstraction

Each ADR must record:

Context.
Problem.
Alternatives considered.
Decision.
Consequences.
Risks.
Date.
29. Initial Development Priorities
The first development iteration should be limited to the storage foundation.

Do not start by building all the crates listed in this document simultaneously.

The initial implementation should contain:

ndb-common
ndb-document
ndb-page
ndb-storage
ndb-buffer
ndb-btree
ndb-wal
ndb-transaction
ndb-server
The first demonstrable product should be a working single-node database that can:

Create a database.
Create a collection.
Insert binary documents.
Retrieve documents by ID.
Update documents.
Delete documents.
Create secondary indexes.
Execute indexed lookups.
Recover from forced termination.
Once this foundation is reliable, the full-text engine can be introduced.

30. Final Architectural Recommendation
NDB should be developed as a collection of independent, carefully defined Rust subsystems.

The most important architectural separation is:

Authoritative document storage

B+Tree, binary documents, WAL, transactions and MVCC.

Derived search infrastructure

Inverted indexes, BM25, fuzzy search, HNSW and hybrid ranking.

Distributed coordination

Raft, sharding, replication, routing and cluster management.

External interfaces

REST, gRPC, SDKs and administration.

This separation will allow each subsystem to be independently benchmarked, optimised and tested.

The database must always be capable of reconstructing its derived indexes from authoritative document data and committed transaction history.

The first engineering objective is not billions of documents or 100 servers. It is a correct, durable, recoverable and measurable single-node storage engine.

Once that foundation is proven, the distributed and search capabilities can be built on top of it without fundamentally changing the storage architecture.

