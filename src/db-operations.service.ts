import {
  Collection,
  Document,
  Filter,
  InsertOneOptions,
  UpdateOptions,
  ReplaceOptions,
  UpdateFilter,
  OptionalUnlessRequiredId,
  ClientSession,
  WithId,
  MongoClient,
  BulkWriteOptions,
  ObjectId,
  EndSessionOptions,
  TransactionOptions,
  ClientSessionOptions,
} from "mongodb";

export interface DbOperationOptions<T> {
  createMetaData?: () => Partial<T>;
  updateMetaData?: () => Partial<T>;
  session?: ClientSession;
  debug?: boolean;
}

export class DbOperationsService<T extends Document = Document> {
  private readonly collection: Collection<T>;
  private readonly client: MongoClient;
  private debug = false;

  constructor(
    collection: Collection<T>,
    client: MongoClient,
    options?: { debug?: boolean }
  ) {
    this.collection = collection;
    this.client = client;
    this.debug = options?.debug ?? false;
  }

  enableDebug(enable = true) {
    this.debug = enable;
  }

  private log(...args: any[]) {
    if (this.debug) console.log("[DbService]", ...args);
  }

  async insertOne(
    doc: OptionalUnlessRequiredId<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const finalDoc = {
      ...doc,
      ...(options.createMetaData?.() ?? {}),
    };

    this.log("insertOne:", finalDoc);
    return await this.collection.insertOne(finalDoc, {
      session: options.session,
    } as InsertOneOptions);
  }

  async insertMany(
    docs: OptionalUnlessRequiredId<T>[],
    options: DbOperationOptions<T> = {}
  ) {
    const finalDocs = docs.map((doc) => ({
      ...doc,
      ...(options.createMetaData?.() ?? {}),
    }));

    return await this.collection.insertMany(finalDocs, {
      session: options.session,
    } as BulkWriteOptions);
  }

  async find(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    this.log("find:", filter);
    return this.collection.find(filter, { session: options.session });
  }

  async findOne(
    filter: Filter<T>,
    options: DbOperationOptions<T> = {}
  ): Promise<WithId<T> | null> {
    this.log("findOne:", filter);
    return await this.collection.findOne(filter, { session: options.session });
  }

  async findById(
    id: string,
    options: DbOperationOptions<T> = {}
  ): Promise<WithId<T> | null> {
    return await this.findOne({ _id: new ObjectId(id) } as Filter<T>, options);
  }

  async patchOne(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("patchOne:", filter, updateDoc);
    return await this.collection.updateOne(filter, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async patchById(
    id: string,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    return await this.patchOne({ _id: id } as Filter<T>, update, options);
  }

  async updateOne(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("updateOne:", filter, updateDoc);
    return await this.collection.updateOne(filter, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async updateMany(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("updateMany:", filter, updateDoc);
    return await this.collection.updateMany(filter, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async replaceOne(
    filter: Filter<T>,
    replacement: T,
    options: DbOperationOptions<T> = {}
  ) {
    this.log("replaceOne:", filter, replacement);

    if (!Object.keys(replacement).length) return null;

    return await this.collection.replaceOne(filter, replacement, {
      session: options.session,
    } as ReplaceOptions);
  }

  async deleteOne(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    this.log("deleteOne:", filter);
    return await this.collection.deleteOne(filter, {
      session: options.session,
    });
  }

  async deleteMany(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    this.log("deleteMany:", filter);
    return await this.collection.deleteMany(filter, {
      session: options.session,
    });
  }

  async softDeleteOne(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    const updateDoc: UpdateFilter<T> = {
      $set: { deletedAt: new Date() } as any,
    };

    return await this.updateOne(filter, updateDoc, options);
  }

  async softDeleteMany(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    const updateDoc: UpdateFilter<T> = {
      $set: { deletedAt: new Date() } as any,
    };

    return await this.updateMany(filter, updateDoc, options);
  }

  async startTransaction(
    sessionOptions?: ClientSessionOptions,
    transactionOptions?: TransactionOptions
  ): Promise<ClientSession> {
    const session = this.client.startSession(sessionOptions);
    session.startTransaction(transactionOptions);
    return session;
  }

  async commitTransaction(session: ClientSession, options?: EndSessionOptions) {
    await session.commitTransaction();
    await session.endSession(options);
  }

  async abortTransaction(session: ClientSession, options?: EndSessionOptions) {
    await session.abortTransaction();
    session.endSession(options);
  }

  async createIndex(
    indexSpec: Parameters<Collection<T>["createIndex"]>[0],
    options?: Parameters<Collection<T>["createIndex"]>[1]
  ) {
    this.log("createIndex:", indexSpec, options);
    return await this.collection.createIndex(indexSpec, options);
  }
}
