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
  InsertManyResult,
} from "mongodb";

export interface DbOperationOptions<T> {
  createMetaData?: () => Partial<T>;
  updateMetaData?: () => Partial<T>;
  session?: ClientSession;
  debug?: boolean;
  includeDeleted?: boolean;
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

  private filterDeleted(
    filter: Filter<T>,
    includeDeleted?: boolean
  ): Filter<T> {
    return includeDeleted
      ? filter
      : ({ $and: [filter, { deletedAt: { $exists: false } }] } as Filter<T>);
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
    const result = await this.collection.insertOne(finalDoc, {
      session: options.session,
    } as InsertOneOptions);
    return result;
  }

  // async insertMany(
  //   docs: OptionalUnlessRequiredId<T>[],
  //   options: DbOperationOptions<T> = {}
  // ) {
  //   const finalDocs = docs.map((doc) => ({
  //     ...doc,
  //     ...(options.createMetaData?.() ?? {}),
  //   }));

  //   this.log("insertMany:", finalDocs);
  //   return this.collection.insertMany(finalDocs, {
  //     session: options.session,
  //   } as InsertManyResult<T>);
  // }

  async findOne(
    filter: Filter<T>,
    options: DbOperationOptions<T> = {}
  ): Promise<WithId<T> | null> {
    const f = this.filterDeleted(filter, options.includeDeleted);
    this.log("findOne:", f);
    return this.collection.findOne(f, { session: options.session });
  }

  async findById(
    id: string,
    options: DbOperationOptions<T> = {}
  ): Promise<WithId<T> | null> {
    return this.findOne({ _id: id } as Filter<T>, options);
  }

  async patchOne(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("patchOne:", f, updateDoc);
    return this.collection.updateOne(f, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async patchById(
    id: string,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    return this.patchOne({ _id: id } as Filter<T>, update, options);
  }

  async updateOne(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("updateOne:", f, updateDoc);
    return this.collection.updateOne(f, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async updateMany(
    filter: Filter<T>,
    update: UpdateFilter<T>,
    options: DbOperationOptions<T> = {}
  ) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    const meta = options.updateMetaData?.();
    const updateDoc = meta
      ? { ...update, $set: { ...(update as any).$set, ...meta } }
      : update;

    this.log("updateMany:", f, updateDoc);
    return this.collection.updateMany(f, updateDoc, {
      session: options.session,
    } as UpdateOptions);
  }

  async replaceOne(
    filter: Filter<T>,
    replacement: T,
    options: DbOperationOptions<T> = {}
  ) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    this.log("replaceOne:", f, replacement);
    return this.collection.replaceOne(f, replacement, {
      session: options.session,
    } as ReplaceOptions);
  }

  async deleteOne(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    this.log("deleteOne:", f);
    return this.collection.deleteOne(f, {
      session: options.session,
    });
  }

  async deleteMany(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
    const f = this.filterDeleted(filter, options.includeDeleted);
    this.log("deleteMany:", f);
    return this.collection.deleteMany(f, {
      session: options.session,
    });
  }

  // async softDeleteOne(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
  //   const updateDoc: UpdateFilter<T> = {$set: { deletedAt: new Date() }};

  //   return this.updateOne(filter, updateDoc as UpdateFilter<T>, options);
  // }

  // async softDeleteMany(filter: Filter<T>, options: DbOperationOptions<T> = {}) {
  //   const updateDoc: UpdateFilter<T> = { $set: { deletedAt: new Date() } };

  //   return this.updateMany(filter, updateDoc as UpdateFilter<T>, options);
  // }

  async startTransaction(): Promise<ClientSession> {
    const session = this.client.startSession();
    session.startTransaction();
    return session;
  }

  async commitTransaction(session: ClientSession) {
    await session.commitTransaction();
    session.endSession();
  }

  async abortTransaction(session: ClientSession) {
    await session.abortTransaction();
    session.endSession();
  }
}
