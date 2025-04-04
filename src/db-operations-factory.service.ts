import { MongoClient, Collection, Document } from "mongodb";
import { DbOperationsService } from "./db-operations.service";

interface FactoryOptions {
  clusterUri: string;
  dbName: string;
  collectionName: string;
  debug?: boolean;
}

export class DbOperationsServiceFactory {
  private static clients: Record<string, MongoClient> = {};

  static async create<T extends Document = Document>(
    options: FactoryOptions
  ): Promise<DbOperationsService<T>> {
    const { clusterUri, dbName, collectionName, debug } = options;

    if (!this.clients[clusterUri]) {
      this.clients[clusterUri] = new MongoClient(clusterUri);
      await this.clients[clusterUri].connect();
    }

    const client = this.clients[clusterUri];
    const db = client.db(dbName);
    const collection = db.collection<T>(collectionName);

    return new DbOperationsService<T>(collection, client, { debug });
  }
}
