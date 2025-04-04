import { MongoClient } from "mongodb";
import { DbOperationsService } from "./db-operations.service";

export async function dbOperationsServiceFactory({
  clusterUrl,
  dbName,
  collectionName,
}: {
  clusterUrl: string;
  dbName: string;
  collectionName: string;
}): Promise<DbOperationsService> {
  const client = new MongoClient(clusterUrl);
  await client.connect();
  const db = client.db(dbName);
  const collection = db.collection(collectionName);
  return new DbOperationsService(collection);
}
