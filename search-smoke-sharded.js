// search-smoke.js — end-to-end check of Percona Search in an mlaunch-go sandbox.
//
// Replica set (connect to the set):
//   mongosh 'mongodb://mlaunch:pa33word@127.0.0.1:28307/?replicaSet=replset&authSource=admin' search-smoke.js
//
// Sharded cluster (connect to a mongos — the script shards the test
// collection so every shard's mongot takes part):
//   mongosh 'mongodb://mlaunch:pa33word@127.0.0.1:28314/?authSource=admin' search-smoke.js
//
// Uses 3-dimensional hand-written vectors, so no embedding provider or API
// key is involved: only mongod -> mongot index management, change-stream
// replication and $vectorSearch / $search routing are exercised.

const t = db.getSiblingDB("searchtest");
const COLL = "fruit";
const coll = t[COLL];

function log(...a) { print("[smoke]", ...a); }

// ---------------------------------------------------------------- 0. plumbing
const admin = t.getSiblingDB("admin");
const hello = admin.runCommand({ hello: 1 });
const sharded = hello.msg === "isdbgrid";

if (sharded) {
  // mongotHost is a mongod parameter: each shard has its own mongot and
  // mongos knows nothing about them, so there is nothing to assert here.
  // A missing mongot shows up in step 2 instead, when createSearchIndexes
  // fails to reach one.
  log("connected to mongos — per-shard mongot wiring is checked via createSearchIndexes");
} else {
  const params = admin.runCommand({
    getParameter: 1, mongotHost: 1, useGrpcForSearch: 1,
  });
  log("mongotHost:", params.mongotHost, "| gRPC:", params.useGrpcForSearch);
  if (!params.mongotHost) {
    throw new Error("mongod has no mongotHost set — this is not a --percona-search sandbox");
  }
}

// ---------------------------------------------------------------- 1. data
coll.drop();

// On a sharded cluster, spread the documents over every shard with a hashed
// key: each shard indexes its own subset, so the queries below only return
// complete results if mongos really is merging per-shard mongot results.
if (sharded) {
  admin.runCommand({ enableSharding: t.getName() });
  const res = admin.runCommand({
    shardCollection: `${t.getName()}.${COLL}`, key: { name: "hashed" },
  });
  if (!res.ok) throw new Error("shardCollection failed: " + tojson(res));
  log("collection sharded on {name: 'hashed'}");
}

coll.insertMany([
  { name: "lemon",     colour: "yellow bright citrus fruit", embedding: [1.0, 0.0, 0.0] },
  { name: "banana",    colour: "yellow soft sweet fruit",    embedding: [0.9, 0.1, 0.0] },
  { name: "lime",      colour: "green sharp citrus fruit",   embedding: [0.7, 0.7, 0.0] },
  { name: "kiwi",      colour: "green fuzzy sweet fruit",    embedding: [0.0, 1.0, 0.0] },
  { name: "blueberry", colour: "blue small sweet berry",     embedding: [0.0, 0.0, 1.0] },
]);
log("inserted", coll.countDocuments(), "documents");

if (sharded) {
  // Which shard holds what, so a one-sided distribution is visible if the
  // per-shard results later look short.
  const stats = coll.aggregate([{ $collStats: { count: {} } }]).toArray();
  log("documents per shard:",
    stats.map((s) => `${s.shard || "?"}=${s.count}`).join(" "));
}

// ---------------------------------------------------------------- 2. indexes
// The raw command works across mongosh versions; the helper equivalent is
// coll.createSearchIndex("vec", "vectorSearch", {...}).
t.runCommand({
  createSearchIndexes: COLL,
  indexes: [
    {
      name: "vec",
      type: "vectorSearch",
      definition: {
        fields: [
          { type: "vector", path: "embedding", numDimensions: 3, similarity: "cosine" },
          { type: "filter", path: "name" },
        ],
      },
    },
    {
      name: "text",
      type: "search",
      definition: { mappings: { dynamic: true } },
    },
  ],
});
log("index build requested");

// ---------------------------------------------------------------- 3. wait
// mongot does an initial sync of the collection before the index answers
// queries; on five documents this is seconds, not minutes.
// On a sharded cluster mongos may report one entry per shard for the same
// index, so count distinct names rather than rows.
const deadline = Date.now() + 120000;
let ready = false;
while (Date.now() < deadline) {
  const idx = t.runCommand({ listSearchIndexes: COLL }).cursor.firstBatch;
  const states = idx.map((i) => `${i.name}=${i.status}`).join(" ");
  const names = new Set(idx.map((i) => i.name));
  if (names.size === 2 && idx.length > 0 && idx.every((i) => i.queryable)) {
    log("indexes queryable:", states);
    ready = true;
    break;
  }
  log("waiting:", states || "(no indexes reported yet)");
  sleep(2000);
}
if (!ready) {
  throw new Error("indexes did not become queryable — check the mongot log in the sandbox data dir");
}

// ---------------------------------------------------------------- 4. vector
const near = coll.aggregate([
  {
    $vectorSearch: {
      index: "vec",
      path: "embedding",
      queryVector: [1.0, 0.05, 0.0], // right next to lemon
      limit: 3,
      // Exhaustive (ENN) search: on five documents the approximate HNSW
      // path can legitimately miss neighbours. numCandidates applies only
      // to the approximate path and must be omitted when exact is true.
      exact: true,
    },
  },
  { $project: { _id: 0, name: 1, score: { $meta: "vectorSearchScore" } } },
]).toArray();

log("$vectorSearch results:");
printjson(near);
if (near.length === 0) throw new Error("$vectorSearch returned nothing");
if (near[0].name !== "lemon") {
  throw new Error(`expected 'lemon' first, got '${near[0].name}'`);
}

// Same query with a filter, to confirm filter fields are indexed.
const filtered = coll.aggregate([
  {
    $vectorSearch: {
      index: "vec",
      path: "embedding",
      queryVector: [1.0, 0.05, 0.0],
      filter: { name: { $ne: "lemon" } },
      limit: 2,
      exact: true,
    },
  },
  { $project: { _id: 0, name: 1, score: { $meta: "vectorSearchScore" } } },
]).toArray();
log("filtered (lemon excluded), expecting banana first:");
printjson(filtered);

// ---------------------------------------------------------------- 5. text
const text = coll.aggregate([
  { $search: { index: "text", text: { query: "citrus", path: "colour" } } },
  { $project: { _id: 0, name: 1, score: { $meta: "searchScore" } } },
]).toArray();
log("$search 'citrus', expecting lemon and lime:");
printjson(text);

// ---------------------------------------------------------------- 6. live sync
// mongot tails the change stream, so a new document should become findable
// without touching the index.
coll.insertOne({ name: "grapefruit", colour: "pink bitter citrus fruit", embedding: [0.95, 0.02, 0.0] });
let synced = false;
for (let i = 0; i < 30; i++) {
  const hits = coll.aggregate([
    { $search: { index: "text", text: { query: "bitter", path: "colour" } } },
    { $project: { _id: 0, name: 1 } },
  ]).toArray();
  if (hits.length > 0) {
    log("change stream replication OK, new doc searchable after ~" + i + "s:", hits.map((h) => h.name).join(", "));
    synced = true;
    break;
  }
  sleep(1000);
}
if (!synced) throw new Error("new document never appeared in the index — replication may be stalled");

log("ALL CHECKS PASSED");
log("clean up with: mongosh <uri> --eval 'db.getSiblingDB(\"searchtest\").dropDatabase()'");
