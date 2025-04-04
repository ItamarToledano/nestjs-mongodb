import {
  Collection,
  Document,
  InsertOneOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  BulkWriteResult,
  Filter,
  UpdateFilter,
  OptionalId,
  WithId,
  BulkWriteOptions,
  ReplaceOptions,
  DeleteOptions,
  FindOptions,
  CountDocumentsOptions,
  AggregateOptions,
  DistinctOptions,
  UpdateOptions,
  AnyBulkWriteOperation,
} from "mongodb";

type MetaDataFunction = () => Record<string, any>;

interface BaseOptions {
  createMetaData?: MetaDataFunction;
}

export class DbOperationsService {
  constructor(private collection: Collection<Document>) {}

  private applyMetaData(
    document: Document,
    metaDataFn?: MetaDataFunction
  ): Document {
    return metaDataFn ? { ...document, ...metaDataFn() } : document;
  }

  async insertOne(
    document: OptionalId<Document>,
    options: InsertOneOptions & BaseOptions = {}
  ): Promise<InsertOneResult<Document>> {
    const docWithMeta = this.applyMetaData(document, options.createMetaData);
    return this.collection.insertOne(docWithMeta, options);
  }

  async insertMany(
    documents: OptionalId<Document>[],
    options: BulkWriteOptions & BaseOptions = {}
  ): Promise<InsertManyResult<Document>> {
    const docsWithMeta = options.createMetaData
      ? documents.map((doc) => this.applyMetaData(doc, options.createMetaData))
      : documents;
    return this.collection.insertMany(docsWithMeta, options);
  }

  async findOne(
    filter: Filter<Document>,
    options: FindOptions = {}
  ): Promise<WithId<Document> | null> {
    return this.collection.findOne(filter, options);
  }

  async findMany(
    filter: Filter<Document> = {},
    options: FindOptions = {}
  ): Promise<WithId<Document>[]> {
    return this.collection.find(filter, options).toArray();
  }

  async updateOne(
    filter: Filter<Document>,
    update: UpdateFilter<Document>,
    options: UpdateOptions & BaseOptions = {}
  ): Promise<UpdateResult> {
    const updateWithMeta = options.createMetaData
      ? {
          ...update,
          $set: { ...(update as any).$set, ...options.createMetaData() },
        }
      : update;
    return this.collection.updateOne(filter, updateWithMeta, options);
  }

  async updateMany(
    filter: Filter<Document>,
    update: UpdateFilter<Document>,
    options: UpdateOptions & BaseOptions = {}
  ): Promise<UpdateResult> {
    const updateWithMeta = options.createMetaData
      ? {
          ...update,
          $set: { ...(update as any).$set, ...options.createMetaData() },
        }
      : update;
    return this.collection.updateMany(filter, updateWithMeta, options);
  }

  async replaceOne(
    filter: Filter<Document>,
    replacement: Document,
    options: ReplaceOptions & BaseOptions = {}
  ): Promise<Document | UpdateResult> {
    const replacementWithMeta = this.applyMetaData(
      replacement,
      options.createMetaData
    );
    return this.collection.replaceOne(filter, replacementWithMeta, options);
  }

  async deleteOne(
    filter: Filter<Document>,
    options: DeleteOptions = {}
  ): Promise<DeleteResult> {
    return this.collection.deleteOne(filter, options);
  }

  async deleteMany(
    filter: Filter<Document>,
    options: DeleteOptions = {}
  ): Promise<DeleteResult> {
    return this.collection.deleteMany(filter, options);
  }

  async aggregate(
    pipeline: Document[],
    options: AggregateOptions = {}
  ): Promise<Document[]> {
    return this.collection.aggregate(pipeline, options).toArray();
  }

  async countDocuments(
    filter: Filter<Document> = {},
    options: CountDocumentsOptions = {}
  ): Promise<number> {
    return this.collection.countDocuments(filter, options);
  }

  async distinct(
    field: string,
    filter: Filter<Document> = {},
    options: DistinctOptions = {}
  ): Promise<any[]> {
    return this.collection.distinct(field, filter, options);
  }

  async bulkWrite(
    operations: AnyBulkWriteOperation[],
    options: BulkWriteOptions = {}
  ): Promise<BulkWriteResult> {
    return this.collection.bulkWrite(operations, options);
  }
}
