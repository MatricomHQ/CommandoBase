import { type } from "os";

export interface GeoPoint {
  lat: number;
  lon: number;
}

export interface ImportItem {
  key: string;
  value: any;
}

export interface BatchSetItem {
    key: string;
    value: any;
}

export type TransactionOperation =
    | { type: 'set'; key: string; value: any }
    | { type: 'delete'; key: string };

export interface CountResponse {
    count: number;
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
    limit?: number;
    offset?: number;
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

interface ClearPrefixPayload {
    prefix: string;
}

export interface DatabaseConfig {
    host: string;
    port: number;
    protocol?: 'http' | 'https';
    cacheTTL?: number;
    apiKey?: string; // Added API key
}

class DatabaseError extends Error {
  public code: number;
  constructor(message: string, code: number) {
    super(message);
    this.name = 'DatabaseError';
    this.code = code;
  }
}

function inferType(value: any): DataType {
  const type = typeof value;
  if (type === 'string') return 'String';
  if (type === 'number') return 'Number';
  if (type === 'boolean') return 'Bool';
  throw new Error(`Unsupported data type for value: ${value} (type: ${type})`);
}

function setValueByPath(obj: any, path: string[], value: any): void {
    if (!path || path.length === 0) {
        throw new Error("Path cannot be empty for setValueByPath");
    }

    let current = obj;
    for (let i = 0; i < path.length - 1; i++) {
        const part = path[i];
        const nextPart = path[i + 1];

        if (part === undefined || part === null) {
             throw new Error(`Invalid path segment at index ${i}`);
        }

        const nextPartIsIndex = nextPart !== undefined && /^\d+$/.test(nextPart);

        if (current[part] === undefined || current[part] === null) {
             current[part] = nextPartIsIndex ? [] : {};
        } else if (nextPartIsIndex && !Array.isArray(current[part])) {
             throw new Error(`Cannot access index '${nextPart}' on non-array value at path '${path.slice(0, i+1).join('.')}'`);
        } else if (!nextPartIsIndex && typeof current[part] !== 'object') {
             throw new Error(`Cannot access property '${nextPart}' on non-object value at path '${path.slice(0, i+1).join('.')}'`);
        }
        current = current[part];
    }

    const lastPart = path[path.length - 1];
    if (lastPart === undefined || lastPart === null) {
        throw new Error("Invalid final path segment");
    }

    if (/^\d+$/.test(lastPart) && !Array.isArray(current)) {
         throw new Error(`Cannot assign to index '${lastPart}' on non-array value at path '${path.slice(0, -1).join('.')}'`);
    }
    current[lastPart] = value;
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

  async exec(limit?: number, offset?: number): Promise<any[]> {
    return this._db._queryAst(this._ast, this._projection, limit, offset);
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
                    if (e instanceof DatabaseError && e.code === 404) {
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
  private cache: Map<string, { value: any; timestamp: number }>;
  private cacheTTL: number;
  private subscriptions: { [key: string]: Array<() => void> };
  private eventSource?: EventSource;
  private apiKey?: string; // Store API Key

  constructor(config?: Partial<DatabaseConfig>) {
    const conf: DatabaseConfig = {
        host: config?.host ?? '127.0.0.1',
        port: config?.port ?? 8989,
        protocol: config?.protocol ?? 'http',
        cacheTTL: config?.cacheTTL ?? 5000,
        apiKey: config?.apiKey, // Store API Key
    };

    if (!conf.host) {
        throw new Error("Database host cannot be empty.");
    }
    if (conf.port <= 0 || conf.port > 65535) {
        throw new Error(`Invalid port number: ${conf.port}`);
    }

    this.baseURL = `${conf.protocol}://${conf.host}:${conf.port}`;
    this.cache = new Map();
    this.cacheTTL = conf.cacheTTL ?? 5000;
    this.subscriptions = {};
    this.apiKey = conf.apiKey; // Store API Key
    console.info(`Database SDK initialized for server at: ${this.baseURL}`);
    this.initializeEventSource(); // Uncommented this line
  }

  private initializeEventSource() {
      try {
          const eventsUrl = `${this.baseURL}/events`;
          console.info(`Attempting to connect to SSE endpoint: ${eventsUrl}`);
          this.eventSource = new EventSource(eventsUrl);

          this.eventSource.onopen = () => {
              console.info(`SSE connection established to ${eventsUrl}`);
          };

          this.eventSource.onerror = (error) => {
              console.error(`SSE connection error to ${eventsUrl}:`, error);
              this.eventSource?.close();

              setTimeout(() => this.initializeEventSource(), 5000);
          };

          this.eventSource.addEventListener('update', (event) => {
              try {
                  console.debug('Received SSE update event:', event.data);
                  const { key } = JSON.parse(event.data);
                  if (key && this.subscriptions[key]) {
                      console.debug(`Notifying subscribers for key: ${key}`);
                      this.subscriptions[key]?.forEach(cb => cb());
                  }
              } catch (e) {
                  console.error('Error processing SSE update event:', e);
              }
          });
      } catch (error) {
          console.error('Failed to initialize EventSource:', error);
      }
  }

  private async _request<T>(endpoint: string, body: any, method: 'POST' | 'GET' = 'POST'): Promise<T> {
    const url = `${this.baseURL}/${endpoint}`;
    const start = performance.now();
    console.debug(`Sending ${method} request to ${url}`, method === 'POST' ? body : '');
    try {
      const headers: HeadersInit = {
        'Content-Type': 'application/json',
      };

      if (this.apiKey) {
        headers['X-API-Key'] = this.apiKey; // Add API Key header
      }

      const response = await fetch(url, {
        method: method,
        headers: headers,
        body: method === 'POST' ? JSON.stringify(body) : undefined,
      });

      const duration = performance.now() - start;
      console.log(`Request to ${endpoint} took ${duration.toFixed(2)}ms`);
      console.debug(`Received response ${response.status} from ${url}`);

      // Special handling for /get 404: return undefined instead of throwing
      if (endpoint === 'get' && response.status === 404) {
          console.debug(`Key not found (404) for ${url}`);
          return undefined as T;
      }

      if (!response.ok) {
        let errorBody;
        let errorMessage = `HTTP error ${response.status} on /${endpoint}`;
        try {
            errorBody = await response.json();
            console.error(`Error response body from ${url}:`, errorBody);
            errorMessage = `Database Error (${response.status}): ${errorBody.error || JSON.stringify(errorBody)}`;
        } catch (e) {
            errorBody = await response.text();
            console.error(`Error response text from ${url}:`, errorBody);
            errorMessage = `HTTP error ${response.status} on /${endpoint}: ${errorBody}`;
        }
        throw new DatabaseError(errorMessage, response.status);
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
      const duration = performance.now() - start;
      console.log(`Request to ${endpoint} took ${duration.toFixed(2)}ms (failed)`);
      console.error(`Request failed for ${url}:`, error);
      if (error instanceof DatabaseError) {
          throw error;
      } else if (error instanceof Error) {
          throw new DatabaseError(`Network or unexpected error for /${endpoint}: ${error.message}`, 0);
      } else {
          throw new DatabaseError(`Unknown error for /${endpoint}`, 0);
      }
    }
  }

  async set(key: string, value: any): Promise<void> {
    await this._request<void>('set', { key, value });
    this.cache.delete(key);
  }

  async get(key: string): Promise<any | undefined> {
    const cached = this.cache.get(key);
    if (cached && Date.now() - cached.timestamp < this.cacheTTL) {
      console.debug(`Cache hit for key: ${key}`);
      return cached.value;
    }
    console.debug(`Cache miss for key: ${key}`);
    // _request now returns undefined for 404 on /get
    const value = await this._request<any | undefined>('get', { key });
    if (value === undefined) {
        // Explicitly throw the expected error type for the tests
        throw new DatabaseError(`Database Error (404): Key not found`, 404);
    }
    this.cache.set(key, { value, timestamp: Date.now() });
    return value;
  }

  async getPartial(key: string, fields: string[]): Promise<any> {
    // Note: getPartial might also need 404 handling if the key itself doesn't exist
    // For now, assume it throws if key not found, or returns partial if key exists but fields don't
    return this._request<any>('get_partial', { key, fields });
  }

  async delete(key: string): Promise<void> {
    await this._request<void>('delete', { key });
    this.cache.delete(key);
  }

  async batchSet(items: BatchSetItem[]): Promise<void> {
      await this._request<void>('batch_set', items);
      items.forEach(item => this.cache.delete(item.key));
  }

  async transaction(operations: TransactionOperation[]): Promise<void> {
      await this._request<void>('transaction', operations);

      operations.forEach(op => {
          if (op.type === 'set' || op.type === 'delete') {
              this.cache.delete(op.key);
          }
      });
  }

  async clearPrefix(prefix: string): Promise<number> {
      const response = await this._request<CountResponse>('clear_prefix', { prefix });

      this.cache.forEach((_, key) => {
          if (key.startsWith(prefix)) {
              this.cache.delete(key);
          }
      });
      return response.count;
  }

  async dropDatabase(): Promise<number> {
      const response = await this._request<CountResponse>('drop_database', {});
      this.cache.clear();
      return response.count;
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

  async _queryAst(ast: AstNode, projection?: string[], limit?: number, offset?: number): Promise<any[]> {
      const payload: QueryAstPayload = { ast };
      if (projection && projection.length > 0) {
          payload.projection = projection;
      }
      if (limit !== undefined) {
          payload.limit = limit;
      }
      if (offset !== undefined) {
          payload.offset = offset;
      }
      return this._request<any[]>('query/ast', payload);
  }

  async exportData(): Promise<string> {
     const dataString = await this._request<string>('export', null, 'GET');
     return dataString;
  }

  async importData(data: ImportItem[]): Promise<void> {
    await this._request<void>('import', data);
    this.cache.clear();
  }

  subscribe(key: string, callback: () => void): () => void {
      this.subscriptions[key] ||= [];
      this.subscriptions[key].push(callback);
      console.debug(`Subscribed to key: ${key}`);

      return () => {
          const currentSubs = this.subscriptions[key];
          if (currentSubs) {
              this.subscriptions[key] = currentSubs.filter(cb => cb !== callback);
              if (this.subscriptions[key].length === 0) {
                  delete this.subscriptions[key];
              }
          }
          console.debug(`Unsubscribed from key: ${key}`);
      };
  }

  collection<T = any>(key: string): CollectionReference<T> {
    if (!key || typeof key !== 'string') {
        throw new Error("Collection key must be a non-empty string.");
    }

    return new Proxy({ db: this, key }, collectionProxyHandler) as unknown as CollectionReference<T>;
  }
}