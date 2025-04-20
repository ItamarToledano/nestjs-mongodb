import { MongoClient, Collection } from "mongodb";
import { DbOperationsService } from "../db-operations.service";
import { MongoMemoryReplSet } from "mongodb-memory-server";

interface TestDoc {
  _id?: string;
  name: string;
  age: number;
  deletedAt?: Date;
}

describe("DbOperationsService", () => {
  let client: MongoClient;
  let service: DbOperationsService<TestDoc>;
  let collection: Collection<TestDoc>;
  let replSet: MongoMemoryReplSet;

  beforeAll(async () => {
    replSet = await MongoMemoryReplSet.create({
      replSet: { count: 1 },
      instanceOpts: [{ storageEngine: "wiredTiger" }],
    });
    const uri = replSet.getUri() + "&retryWrites=false";
    client = new MongoClient(uri);
    await client.connect();

    const db = client.db("testdb");
    collection = db.collection<TestDoc>("testCollection");
    service = new DbOperationsService(collection, client);
  });

  afterAll(async () => {
    if (client) await client.close();
    if (replSet) await replSet.stop();
  });

  beforeEach(async () => {
    await collection.deleteMany({});
  });

  it("inserts one document", async () => {
    const result = await service.insertOne({ name: "Alice", age: 30 });
    expect(result.acknowledged).toBeTruthy();
    const doc = await collection.findOne({ name: "Alice" });
    expect(doc).toBeTruthy();
    expect(doc?.age).toBe(30);
  });

  it("inserts many documents", async () => {
    const result = await service.insertMany([
      { name: "Bob", age: 25 },
      { name: "Carol", age: 28 },
    ]);
    expect(result.insertedCount).toBe(2);
  });

  it("finds one document", async () => {
    await collection.insertOne({ name: "David", age: 22 });
    const doc = await service.findOne({ name: "David" });
    expect(doc?.age).toBe(22);
  });

  it("finds by ID", async () => {
    const { insertedId } = await collection.insertOne({ name: "Eve", age: 27 });
    const doc = await service.findById(insertedId.toString());
    expect(doc?.name).toBe("Eve");
  });

  it("updates one document", async () => {
    await collection.insertOne({ name: "Frank", age: 35 });
    const res = await service.updateOne(
      { name: "Frank" },
      { $set: { age: 36 } }
    );
    expect(res.modifiedCount).toBe(1);
    const updated = await collection.findOne({ name: "Frank" });
    expect(updated?.age).toBe(36);
  });

  it("patches one document", async () => {
    await collection.insertOne({ name: "Grace", age: 40 });
    await service.patchOne({ name: "Grace" }, { $set: { age: 41 } });
    const patched = await collection.findOne({ name: "Grace" });
    expect(patched?.age).toBe(41);
  });

  it("replaces one document", async () => {
    await collection.insertOne({ name: "Henry", age: 50 });
    await service.replaceOne({ name: "Henry" }, { name: "Henry New", age: 51 });
    const found = await collection.findOne({ name: "Henry New" });
    expect(found?.age).toBe(51);
  });

  it("deletes one document", async () => {
    await collection.insertOne({ name: "Ivy", age: 20 });
    await service.deleteOne({ name: "Ivy" });
    const deleted = await collection.findOne({ name: "Ivy" });
    expect(deleted).toBeNull();
  });

  it("soft deletes one document", async () => {
    await collection.insertOne({ name: "Jack", age: 33 });
    await service.softDeleteOne({ name: "Jack" });
    const softDeleted = await collection.findOne({ name: "Jack" });
    expect(softDeleted?.deletedAt).toBeInstanceOf(Date);
  });

  it("soft deletes many documents", async () => {
    await collection.insertMany([
      { name: "Ken", age: 18 },
      { name: "Ken", age: 19 },
    ]);
    const res = await service.softDeleteMany({ name: "Ken" });
    expect(res.modifiedCount).toBe(2);
  });

  it("creates an index", async () => {
    const indexName = await service.createIndex({ name: 1 });
    const indexes = await collection.indexes();
    expect(indexes.map((i) => i.name)).toContain(indexName);
  });

  it("handles transactions", async () => {
    const isReplicaSet = !!client.options.replicaSet;

    if (!isReplicaSet) {
      console.warn(
        "Skipping transaction test because replica set is not enabled"
      );
      return;
    }

    const session = await service.startTransaction();
    await service.insertOne({ name: "Leo", age: 29 }, { session });
    await expect(service.commitTransaction(session)).resolves.not.toThrow();

    const inserted = await collection.findOne({ name: "Leo" });
    expect(inserted).toBeTruthy();
    expect(inserted?.name).toBe("Leo");

    await session.endSession();
  });

  it("aborts transactions", async () => {
    const isReplicaSet = !!client.options.replicaSet;

    if (!isReplicaSet) {
      console.warn(
        "Skipping transaction abort test because replica set is not enabled"
      );
      return;
    }

    const session = await service.startTransaction();
    await service.insertOne({ name: "Mike", age: 32 }, { session });
    await expect(service.abortTransaction(session)).resolves.not.toThrow();

    const notInserted = await collection.findOne({ name: "Mike" });
    expect(notInserted).toBeNull();

    await session.endSession();
  });
});
