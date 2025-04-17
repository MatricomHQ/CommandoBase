import { type } from "os";

export interface GeoPoint {
  lat: number;
  lon: number;
}

export interface ImportItem {
  key: string;
  value: any;
}

export type DataType = 'String' | 'Number' | 'Bool';

export type AstNode =
  | { Eq: [string, any, DataType] }
  | { Includes: [string, any, DataType] }
  | { Gt: [string, any, DataType] }
  | { Lt: [string, any, DataType] }
  | { Gte: [string, any, DataType] }
  | { Lte: [string, any, DataType] }
  | { Ne: [string, any, DataType] }
  | { And: [AstNode, AstNode] }
  | { Or: [AstNode, AstNode] }
  | { Not: AstNode }
  | { GeoWithinRadius: { field: string; lat: number; lon: number; radius: number } }
  | { GeoInBox: { field: string; min_lat: number; min_lon: number; max_lat: number; max_lon: number } };

interface QueryAstPayload {
    ast: AstNode;
    projection?: string[];
}

interface SetPayload {
    key: string;
    value: any;
}

interface KeyPayload {
    key: string;
}

interface GetPartialPayload {
    key: string;
    fields: string[];
}

interface QueryRadiusPayload {
    field: string;
    lat: number;
    lon: number;
    radius: number;
}

interface QueryBoxPayload {
    field: string;
    min_lat: number;
    min_lon: number;
    max_lat: number;
    max_lon: number;
}

export interface DatabaseConfig {
    host: string;
    port: number;
    protocol?: 'http' | 'https';
}


function inferType(value: any): DataType {
  const type = typeof value;
  if (type === 'string') return 'String';
  if (type === 'number') return 'Number';
  if (type === 'boolean') return 'Bool';
  throw new Error(`Unsupported data type for value: ${value} (type: ${type})`);
}

function setValueByPath(obj: any, path: string[], value: any): void {
    let current = obj;
    for (let i = 0; i < path.length - 1; i++) {
        const part = path[i];
        if (current[part] === undefined || typeof current[part] !== 'object' || current[part] === null) {
            const nextPart = path[i+1];
            current[part] = /^\d+$/.test(nextPart) ? [] : {};
        }
        current = current[part];
    }
    current[path[path.length - 1]] = value;
}

class Condition {
  private _db: Database;
  private _ast: AstNode;
  private _projection?: string[];

  constructor(db: Database, ast: AstNode, projection?: string[]) {
    this._db = db;
    this._ast = ast;
    this._projection = projection;
  }

  toAST(): AstNode {
    return this._ast;
  }

  and(other: Condition): Condition {
    if (!(other instanceof Condition)) {
        throw new Error("Argument to 'and' must be a Condition object.");
    }
    const newAst: AstNode = { And: [this.toAST(), other.toAST()] };
    return new Condition(this._db, newAst, this._projection);
  }

  or(other: Condition): Condition {
     if (!(other instanceof Condition)) {
         throw new Error("Argument to 'or' must be a Condition object.");
     }
    const newAst: AstNode = { Or: [this.toAST(), other.toAST()] };
    return new Condition(this._db, newAst, this._projection);
  }

  not(): Condition {
    const newAst: AstNode = { Not: this.toAST() };
    return new Condition(this._db, newAst, this._projection);
  }

  select(...fields: string[]): this {
    this._projection = fields;
    return this;
  }

  async exec(): Promise<any[]> {
    return this._db._queryAst(this._ast, this._projection);
  }
}

const fieldProxyHandler: ProxyHandler<{ db: Database; path: string[] }> = {
    get(target, prop, receiver) {
        const currentPath = target.path.join('.');

        switch (prop) {
            case 'eq':
                return (value: any) => new Condition(target.db, { Eq: [currentPath, value, inferType(value)] });
            case 'ne':
                return (value: any) => new Condition(target.db, { Ne: [currentPath, value, inferType(value)] });
            case 'gt':
                return (value: number | string) => new Condition(target.db, { Gt: [currentPath, value, inferType(value)] });
            case 'gte':
                return (value: number | string) => new Condition(target.db, { Gte: [currentPath, value, inferType(value)] });
            case 'lt':
                return (value: number | string) => new Condition(target.db, { Lt: [currentPath, value, inferType(value)] });
            case 'lte':
                return (value: number | string) => new Condition(target.db, { Lte: [currentPath, value, inferType(value)] });
            case 'includes':
                return (value: any) => new Condition(target.db, { Includes: [currentPath, value, inferType(value)] });
            case 'withinRadius':
                return (lat: number, lon: number, radius: number) => new Condition(target.db, { GeoWithinRadius: { field: currentPath, lat, lon, radius } });
            case 'inBox':
                return (min_lat: number, min_lon: number, max_lat: number, max_lon: number) => new Condition(target.db, { GeoInBox: { field: currentPath, min_lat, min_lon, max_lat, max_lon } });
            case 'then':
            case 'catch':
            case 'finally':
                 return undefined;
        }

        if (typeof prop === 'string') {
            const newPath = [...target.path, prop];
            return new Proxy({ db: target.db, path: newPath }, fieldProxyHandler);
        }

        return Reflect.get(target, prop, receiver);
    }
};

const COLLECTION_KEY = Symbol('collectionKey');

const collectionProxyHandler: ProxyHandler<{ db: Database; key: string }> = {
    get(target, prop, receiver) {
        if (prop === COLLECTION_KEY) {
            return target.key;
        }

        if (typeof prop === 'string') {
             if (prop === 'then' || prop === 'catch' || prop === 'finally') {
                 return undefined;
             }
            const path = [prop];
            return new Proxy({ db: target.db, path }, fieldProxyHandler);
        }

        return Reflect.get(target, prop, receiver);
    },
    set(target, prop, value, receiver) {
        if (typeof prop !== 'string') {
            throw new Error("Can only set string properties.");
        }
        const key = target.key;
        const path = [prop];


        (async () => {
            try {
                let currentDoc = {};
                try {
                    currentDoc = await target.db.get(key) || {};
                } catch (e: any) {
                    if (e?.message?.includes("Key not found") || e?.message?.includes("HTTP error 404")) {
                         currentDoc = {};
                    } else {
                        console.error(`Error fetching document for set operation on key '${key}':`, e);
                        throw e;
                    }
                }
                setValueByPath(currentDoc, path, value);
                await target.db.set(key, currentDoc);
            } catch (error) {
                console.error(`Failed to set property '${prop}' on key '${key}':`, error);

            }
        })();


        return true;
    }
};

export type CollectionReference<T = any> = {
    [K in keyof T]: QueryBuilder<T[K]>;
} & {
    [key: string]: QueryBuilder<any>;
};

type QueryBuilder<T> = T extends Array<infer U>
  ? ArrayQueryBuilder<U>
  : T extends object
  ? ObjectQueryBuilder<T>
  : PrimitiveQueryBuilder<T>;

type PrimitiveQueryBuilder<T> = {
  eq(value: T): Condition;
  ne(value: T): Condition;
  gt(value: T): Condition;
  gte(value: T): Condition;
  lt(value: T): Condition;
  lte(value: T): Condition;
} & (T extends GeoPoint ? GeoQueryBuilder : {});

type ArrayQueryBuilder<T> = {
  includes(value: T): Condition;
};

type ObjectQueryBuilder<T> = {
  [K in keyof T]: QueryBuilder<T[K]>;
};

type GeoQueryBuilder = {
    withinRadius(lat: number, lon: number, radius: number): Condition;
    inBox(minLat: number, minLon: number, maxLat: number, maxLon: number): Condition;
};


export class Database {
  private baseURL: string;

  constructor(config?: Partial<DatabaseConfig>) {
    const conf: DatabaseConfig = {
        host: config?.host ?? '127.0.0.1',
        port: config?.port ?? 8989,
        protocol: config?.protocol ?? 'http',
    };

    if (!conf.host) {
        throw new Error("Database host cannot be empty.");
    }
    if (conf.port <= 0 || conf.port > 65535) {
        throw new Error(`Invalid port number: ${conf.port}`);
    }

    this.baseURL = `${conf.protocol}://${conf.host}:${conf.port}`;
    console.info(`Database SDK initialized for server at: ${this.baseURL}`);
  }

  private async _request<T>(endpoint: string, body: any): Promise<T> {
    const url = `${this.baseURL}/${endpoint}`;
    console.debug(`Sending request to ${url}`, body);
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      console.debug(`Received response ${response.status} from ${url}`);

      if (!response.ok) {
        let errorBody;
        try {
            errorBody = await response.json();
            console.error(`Error response body from ${url}:`, errorBody);
        } catch (e) {
            errorBody = await response.text();
            console.error(`Error response text from ${url}:`, errorBody);
        }
        throw new Error(`HTTP error ${response.status} on /${endpoint}: ${typeof errorBody === 'string' ? errorBody : JSON.stringify(errorBody)}`);
      }

      if (response.status === 204 || response.headers.get('content-length') === '0') {
          console.debug(`Empty response body from ${url}`);
          return undefined as T;
      }

      const contentType = response.headers.get('content-type');
       if (contentType && contentType.includes('application/json')) {
           const jsonData = await response.json();
           console.debug(`JSON response body from ${url}:`, jsonData);
           return jsonData as T;
       } else {
            const textData = await response.text();
            console.debug(`Text response body from ${url}:`, textData);
            return textData as T;
       }

    } catch (error) {
      console.error(`Request failed for ${url}:`, error);
      throw error;
    }
  }

  async set(key: string, value: any): Promise<void> {
    await this._request<void>('set', { key, value });
  }

  async get(key: string): Promise<any> {
    return this._request<any>('get', { key });
  }

  async getPartial(key: string, fields: string[]): Promise<any> {
    return this._request<any>('get_partial', { key, fields });
  }

  async delete(key: string): Promise<void> {
    await this._request<void>('delete', { key });
  }

  async queryRadius(payload: QueryRadiusPayload): Promise<any[]> {
     return this._request<any[]>('query/radius', payload);
  }

  async queryBox(payload: QueryBoxPayload): Promise<any[]> {
      return this._request<any[]>('query/box', payload);
  }

  async queryAnd(conditions: [string, string, string][]): Promise<any[]> {
      return this._request<any[]>('query/and', { conditions });
  }

  async _queryAst(ast: AstNode, projection?: string[]): Promise<any[]> {
      const payload: QueryAstPayload = { ast };
      if (projection && projection.length > 0) {
          payload.projection = projection;
      }
      return this._request<any[]>('query/ast', payload);
  }

  async exportData(): Promise<string> {
     const url = `${this.baseURL}/export`;
     console.debug(`Sending request to ${url}`);
     const response = await fetch(url);
      if (!response.ok) {
          console.error(`Error response ${response.status} from ${url}`);
          throw new Error(`HTTP error ${response.status} on /export`);
      }
      const dataString = await response.json();
      console.debug(`Received export data from ${url}`);
      return dataString;
  }

  async importData(data: ImportItem[]): Promise<void> {
    await this._request<void>('import', data);
  }

  collection<T = any>(key: string): CollectionReference<T> {
    if (!key || typeof key !== 'string') {
        throw new Error("Collection key must be a non-empty string.");
    }
    // Cast through unknown to satisfy TypeScript's strict checking for Proxy return types
    return new Proxy({ db: this, key }, collectionProxyHandler) as unknown as CollectionReference<T>;
  }
}