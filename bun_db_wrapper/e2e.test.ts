/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import type { Subprocess } from "bun";
import { resolve } from 'path';
import { Database } from "../sdk"; // Import SDK
import type { BatchSetItem, TransactionOperation, AstNode } from "../sdk";

// --- Test Configuration ---
const RUST_SERVER_PORT = 8989; // Use the same port as the SDK expects
const TEST_DB_BASE_PATH = "test_database_data_server_e2e"; // Use a separate DB dir for E2E tests
const TEST_DB_NAME = "e2e_test_db";
const WORKSPACE_ROOT = resolve(import.meta.dir, "../");
const RUST_SERVER_BINARY = resolve(WORKSPACE_ROOT, "target/release/rust_db_server");
const RUST_SERVER_HEALTH_URL = `http://localhost:${RUST_SERVER_PORT}/`;
const SERVER_START_DELAY_MS = 5000; // Allow time for server to start
const API_TIMEOUT_MS = 15000; // Timeout for individual operations
const TEST_API_KEY = "testapikey123"; // Define a test API key

// --- Test Setup & Teardown ---
let rustServerProcess: Subprocess | null = null;
let db: Database;

async function cleanupTestDB() {
    console.log(`Attempting to clean up E2E test database at ${TEST_DB_BASE_PATH}...`);
    try {
        const fs = await import("fs/promises");
        await fs.rm(resolve(WORKSPACE_ROOT, TEST_DB_BASE_PATH), { recursive: true, force: true });
        console.log(`Cleaned up E2E test database at ${TEST_DB_BASE_PATH}.`);
    } catch (err: any) {
        if (err.code === 'ENOENT') {
            console.log(`E2E Test database directory not found at ${TEST_DB_BASE_PATH}, skipping cleanup.`);
        } else {
            console.error("Error cleaning up E2E test database:", err);
        }
    }
}

async function waitFixedDelay(ms: number, serverName: string): Promise<void> {
    console.log(`Waiting ${ms / 1000}s for ${serverName} to start (fixed delay)...`);
    await new Promise(resolve => setTimeout(resolve, ms));
    console.log(`${serverName} start delay complete.`);
}

async function killProcess(process: Subprocess | null, name: string) {
    if (!process || !process.pid) {
        console.log(`${name} process not found or no PID.`);
        return;
    }
    const pid = process.pid;
    console.log(`Attempting to kill ${name} process ${pid}...`);
    try {
        process.kill(0); // Check if process exists
        console.log(`Sending SIGTERM to ${name} process ${pid}...`);
        process.kill();
        await Promise.race([
            process.exited,
            new Promise(resolve => setTimeout(resolve, 1500))
        ]);
        process.kill(0); // Re-check if process still exists
        console.warn(`${name} process ${pid} did not exit gracefully. Sending SIGKILL...`);
        process.kill(9);
        await process.exited;
        console.log(`${name} process ${pid} killed.`);
    } catch (e: any) {
        if (e.code === 'ESRCH') {
            console.log(`{name} process ${pid} already exited.`);
        } else {
            console.error(`Error killing ${name} process ${pid}:`, e);
            console.log(`Attempting fallback kill for PID ${pid}`);
            Bun.spawnSync({ cmd: ["kill", "-9", `${pid}`], stdout: "ignore", stderr: "ignore" });
        }
    }
}

async function startRustServer(): Promise<Subprocess> {
    console.log("Spawning Rust server process for E2E tests...");
    const proc = Bun.spawn({
        cmd: [RUST_SERVER_BINARY],
        cwd: WORKSPACE_ROOT,
        stdout: "inherit",
        stderr: "inherit",
        env: {
            ...Bun.env,
            "DB_PATH": TEST_DB_BASE_PATH,
            "DB_NAME": TEST_DB_NAME,
            "LISTEN_ADDR": `127.0.0.1:${RUST_SERVER_PORT}`,
            "RUST_LOG": "rust_db_server=info,tower_http=warn", // Adjust log level if needed
            "DB_API_KEY": TEST_API_KEY, // Pass the test API key
        },
    });
    if (!proc?.pid) throw new Error("Rust server process failed to spawn.");
    console.log(`Rust server process spawned with PID: ${proc.pid}`);
    await waitFixedDelay(SERVER_START_DELAY_MS, "Rust server");
    try {
        const res = await fetch(RUST_SERVER_HEALTH_URL, { signal: AbortSignal.timeout(2000) });
        if (!res.ok) throw new Error(`Rust server health check failed: ${res.status}`);
        console.log("Rust server health check passed.");
    } catch (e) {
        console.error("Rust server failed initial health check:", e);
        throw new Error("Rust server did not become healthy.");
    }
    return proc;
}

// --- Global Setup & Teardown ---
beforeAll(async () => {
    console.log("Starting GLOBAL E2E test setup...");
    await cleanupTestDB(); // Initial cleanup
    console.log("Attempting GLOBAL E2E pre-emptive kill...");
    try {
        // Kill any potentially lingering server processes from previous runs
        Bun.spawnSync({ cmd: ["pkill", "-9", "-f", RUST_SERVER_BINARY], stdout: "ignore", stderr: "ignore" });
        console.log("GLOBAL E2E pre-emptive kill attempt complete.");
        await new Promise(resolve => setTimeout(resolve, 500));
    } catch (e) {
        console.warn("Could not execute pkill, skipping GLOBAL E2E pre-emptive kill.");
    }
    console.log("Building Rust server (once for E2E)...");
    const buildProcess = Bun.spawnSync({
        cmd: ["cargo", "build", "--release"],
        cwd: WORKSPACE_ROOT,
        stdout: "inherit",
        stderr: "inherit",
    });
    if (buildProcess.exitCode !== 0) {
        throw new Error(`Failed to build Rust server. Exit code: ${buildProcess.exitCode}`);
    }
    console.log("Rust server built.");
    rustServerProcess = await startRustServer();
    // Instantiate SDK
    db = new Database({ port: RUST_SERVER_PORT, apiKey: TEST_API_KEY }); // Pass the API key to the SDK
    console.log("GLOBAL E2E test setup complete.");
});

afterAll(async () => {
    console.log("Starting GLOBAL E2E test teardown...");
    await killProcess(rustServerProcess, "Rust server");
    await cleanupTestDB(); // Final cleanup
    console.log("GLOBAL E2E test teardown complete.");
});

// --- Test Suites ---
describe("SDK E2E Tests", () => {
    // Clean the database before each test
    beforeEach(async () => {
        console.log("Dropping database before test...");
        try {
            await db.dropDatabase();
            console.log("Database dropped.");
        } catch (e) {
            console.warn("Failed to drop database (might be empty already):", e);
        }
        await new Promise(resolve => setTimeout(resolve, 50)); // Short delay after drop
    });

    it("should perform batchSet correctly", async () => {
        const items: BatchSetItem[] = [
            { key: "batch1", value: { name: "Item 1", count: 10 } },
            { key: "batch2", value: "Simple string" },
            { key: "batch3", value: [1, 2, 3] },
        ];

        await db.batchSet(items);

        let val1: any | undefined = await db.get("batch1");
        // @ts-ignore
        expect(val1).toEqual(items[0].value);

        let val2: any | undefined = await db.get("batch2");
         // @ts-ignore
        expect(val2).toEqual(items[1].value);

        let val3: any | undefined = await db.get("batch3");
         // @ts-ignore
        expect(val3).toEqual(items[2].value);
    });

    it("should execute a transaction correctly", async () => {
        await db.set("tx_delete_me", "initial value");
        const operations: TransactionOperation[] = [
            { type: 'set', key: 'tx_set_key', value: { status: "set in transaction" } },
            { type: 'delete', key: 'tx_delete_me' },
            { type: 'set', key: 'tx_another_set', value: 12345 },
        ];
        await db.transaction(operations);

        const setVal = await db.get("tx_set_key");
        expect(setVal).toEqual({ status: "set in transaction" });

        const anotherVal = await db.get("tx_another_set");
        expect(anotherVal).toEqual(12345);

        await expect(db.get("tx_delete_me")).rejects.toThrow(/Key not found|HTTP error 404/);
    });

    it("should clear keys by prefix", async () => {
        await db.set("prefix/key1", "value1");
        await db.set("prefix/key2", { nested: true });
        await db.set("prefix/deep/key3", [1, 2]);
        await db.set("other_key", "should remain");

        const count = await db.clearPrefix("prefix/");
        expect(count).toBe(3);

        await expect(db.get("prefix/key1")).rejects.toThrow(/Key not found|HTTP error 404/);
        await expect(db.get("prefix/key2")).rejects.toThrow(/Key not found|HTTP error 404/);
        await expect(db.get("prefix/deep/key3")).rejects.toThrow(/Key not found|HTTP error 404/);

        const otherVal = await db.get("other_key");
        expect(otherVal).toBe("should remain");
    });

    it("should drop the database", async () => {
        await db.set("drop_key1", "abc");
        await db.set("drop_key2", 123);

        const count = await db.dropDatabase();
        expect(count).toBeGreaterThanOrEqual(2); // Might include internal keys if any

        await expect(db.get("drop_key1")).rejects.toThrow(/Key not found|HTTP error 404/);
        await expect(db.get("drop_key2")).rejects.toThrow(/Key not found|HTTP error 404/);

        // Try setting again to ensure DB is usable after drop
        await db.set("after_drop", "it works");
        const val = await db.get("after_drop");
        expect(val).toBe("it works");
    });

    it("should handle pagination with queryAst", async () => {
        // Set up data
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < 15; i++) {
            const key = `page_item_${i}`;
            const value = { type: "pagination_test", index: i, name: `Item ${i}` };
            console.log(`Setting key: ${key} with value: ${JSON.stringify(value)}`);
            itemsToSet.push({ key, value });
        }
        await db.batchSet(itemsToSet);

        // Verify data in database
        for (let i = 0; i < 15; i++) {
            const key = `page_item_${i}`;
            const value = await db.get(key);
            console.log(`Verifying key: ${key} has value: ${JSON.stringify(value)}`);
        }

        const queryAst: AstNode = { Eq: ["type", "pagination_test", "String"] };

        // Test limit
        const resultsLimit = await db._queryAst(queryAst, undefined, 5);
        expect(resultsLimit.length).toBe(5);

        // Test limit + offset
        const resultsOffset = await db._queryAst(queryAst, undefined, 5, 7);
        expect(resultsOffset.length).toBe(5);

        // Test limit exceeding remaining items
        const resultsOffsetEnd = await db._queryAst(queryAst, undefined, 5, 12);
        expect(resultsOffsetEnd.length).toBe(3); // Only 3 items left (12, 13, 14)

        // Test offset exceeding total items
        const resultsOffsetBeyond = await db._queryAst(queryAst, undefined, 5, 20);
        expect(resultsOffsetBeyond.length).toBe(0);
    });

    // --- New Tests ---

    it("should query based on a deeply nested field", async () => {
        const users = [
            { key: "user1", value: { type: "user", name: "Alice", profile: { settings: { notifications: { email: { enabled: true }, sms: false } } } } },
            { key: "user2", value: { type: "user", name: "Bob", profile: { settings: { notifications: { email: { enabled: false }, sms: true } } } } },
            { key: "user3", value: { type: "user", name: "Charlie", profile: { settings: { notifications: { email: { enabled: true }, sms: true } } } } },
            { key: "user4", value: { type: "user", name: "David", profile: { settings: { notifications: { email: { enabled: false }, sms: false } } } } }
        ];
        await db.batchSet(users);

        // Query for users with email notifications enabled
        const queryAst: AstNode = { Eq: ["profile.settings.notifications.email.enabled", true, "Bool"] };
        const results = await db._queryAst(queryAst);

        expect(results.length).toBe(2);
        const names = new Set(results.map(u => u.name));
        expect(names).toEqual(new Set(["Alice", "Charlie"]));
    });

    it("should perform a simulated join using projection on nested objects", async () => {
        const posts = [
            { key: "post1", value: { type: "post", title: "First Post", content: "...", author: { id: "author1", name: "Alice A", role: "Admin" } } },
            { key: "post2", value: { type: "post", title: "Second Post", content: "...", author: { id: "author2", name: "Bob B", role: "Editor" } } },
            { key: "post3", value: { type: "post", title: "Third Post", content: "...", author: { id: "author1", name: "Alice A", role: "Admin" } } },
        ];
        await db.batchSet(posts);

        // Query for posts by Alice (author1) and project title and author's name
        const queryAst: AstNode = { Eq: ["author.id", "author1", "String"] };
        const projection = ["title", "author.name"];
        const results = await db._queryAst(queryAst, projection);

        expect(results.length).toBe(2);

        // Sort results by title for consistent checking
        results.sort((a, b) => a.title.localeCompare(b.title));

        // Check first result (First Post)
        expect(results[0]).toEqual({
            title: "First Post",
            author: { name: "Alice A" }
        });
        expect(Object.keys(results[0]).length).toBe(2); // Ensure only projected fields exist
        expect(Object.keys(results[0].author).length).toBe(1); // Ensure only projected author fields exist

        // Check second result (Third Post)
        expect(results[1]).toEqual({
            title: "Third Post",
            author: { name: "Alice A" }
        });
         expect(Object.keys(results[1]).length).toBe(2);
         expect(Object.keys(results[1].author).length).toBe(1);
    });

    it("should handle queryAst with empty projection", async () => {
        // Set up data
        await db.set("test_key", { name: "Test Item", value: 123 });

        const queryAst: AstNode = { Eq: ["name", "Test Item", "String"] };
        const results = await db._queryAst(queryAst, []);

        expect(results.length).toBe(1);
    });

    it("should handle queryAst with non-existent field in projection", async () => {
        // Set up data
        await db.set("test_key", { name: "Test Item", value: 123 });

        const queryAst: AstNode = { Eq: ["name", "Test Item", "String"] };
        const results = await db._queryAst(queryAst, ["nonExistentField"]);

        expect(results.length).toBe(1);
    });

    it("should handle queryAst with large limit and offset", async () => {
        // Set up data
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < 5; i++) {
            const key = `large_page_item_${i}`;
            const value = { type: "large_pagination_test", index: i, name: `Item ${i}` };
            itemsToSet.push({ key, value });
        }
        await db.batchSet(itemsToSet);

        const queryAst: AstNode = { Eq: ["type", "large_pagination_test", "String"] };
        const results = await db._queryAst(queryAst, undefined, 1000, 5000);

        expect(results.length).toBe(0);
    });

    it("should handle empty batchSet", async () => {
        await db.batchSet([]);
    });

    it("should handle batchSet with very large values", async () => {
        const longString = "A".repeat(10000);
        const largeNumber = 12345678901234567890;

        const items: BatchSetItem[] = [
            { key: "long_string_key", value: longString },
            { key: "large_number_key", value: largeNumber },
        ];

        await db.batchSet(items);

        const stringValue = await db.get("long_string_key");
        expect(stringValue).toBe(longString);

        const numberValue = await db.get("large_number_key");
        expect(numberValue).toBe(largeNumber);
    });

    it("should handle clearPrefix with empty prefix", async () => {
        const initialCount = await db.dropDatabase();
        const count = await db.clearPrefix("");
        expect(count).toBe(initialCount);
    });

    it("should handle clearPrefix with non-existent prefix", async () => {
        const count = await db.clearPrefix("nonExistentPrefix/");
        expect(count).toBe(0);
    });

});

describe("Benchmarking Tests", () => {
    const NUM_OPERATIONS = 1000; // Number of operations to perform for each benchmark
    const DEEP_NESTING_LEVEL = 5;
    const benchmarkResults: { [key: string]: number } = {};

    beforeEach(async () => {
        console.log("Dropping database before benchmark...");
        try {
            await db.dropDatabase();
            console.log("Database dropped.");
        } catch (e) {
            console.warn("Failed to drop database (might be empty already):", e);
        }
        await new Promise(resolve => setTimeout(resolve, 50)); // Short delay after drop
    });

    afterAll(() => {
        console.log("--- Benchmark Results ---");
        for (const key in benchmarkResults) {
            if (benchmarkResults[key]) {
                console.log(`${key}: ${benchmarkResults[key].toFixed(4)}ms`);
            }
        }
    });

    it("benchmark: set", async () => {
        const start = performance.now();
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.set(`benchmark_key_${i}`, i);
        }
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["set"] = duration;
        console.log(`set: Average time per operation = ${duration.toFixed(4)}ms`);

        // Verify that the keys were set
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const value = await db.get(`benchmark_key_${i}`);
            expect(value).toBe(i);
        }
    });

    it("benchmark: set (deep nesting)", async () => {
        const start = performance.now();
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            let nestedValue: any = i;
            let key = `benchmark_deep_set_key_${i}`;
            for (let j = 0; j < DEEP_NESTING_LEVEL; j++) {
                nestedValue = { [`level${j}`]: nestedValue };
            }
            await db.set(key, nestedValue);
        }
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["set (deep nesting)"] = duration;
        console.log(`set (deep nesting): Average time per operation = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: get", async () => {
        // First, populate the database
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.set(`benchmark_get_key_${i}`, i);
        }

        const start = performance.now();
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.get(`benchmark_get_key_${i}`);
        }
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["get"] = duration;
        console.log(`get: Average time per operation = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: get (deep nesting)", async () => {
        // First, populate the database with deeply nested values
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            let nestedValue: any = i;
            let key = `benchmark_deep_get_key_${i}`;
            for (let j = 0; j < DEEP_NESTING_LEVEL; j++) {
                nestedValue = { [`level${j}`]: nestedValue };
            }
            await db.set(key, nestedValue);
        }

        const start = performance.now();
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.get(`benchmark_deep_get_key_${i}`);
        }
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["get (deep nesting)"] = duration;
        console.log(`get (deep nesting): Average time per operation = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: getPartial", async () => {
        // First, populate the database
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.set(`benchmark_partial_key_${i}`, { name: `Item ${i}`, value: i, description: `Description ${i}` });
        }

        const start = performance.now();
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.getPartial(`benchmark_partial_key_${i}`, ["name", "value"]);
        }
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["getPartial"] = duration;
        console.log(`getPartial: Average time per operation = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: batchSet", async () => {
        const items: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            items.push({ key: `benchmark_batch_key_${i}`, value: i });
        }

        const start = performance.now();
        await db.batchSet(items);
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["batchSet"] = duration;
        console.log(`batchSet: Average time per operation = ${duration.toFixed(4)}ms`);

        // Verify that the keys were set
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const value = await db.get(`benchmark_batch_key_${i}`);
            expect(value).toBe(i);
        }
    });

    it("benchmark: batchSet (large values)", async () => {
        const longString = "A".repeat(1000); // Reduced for performance
        const items: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            items.push({ key: `benchmark_batch_large_key_${i}`, value: longString });
        }

        const start = performance.now();
        await db.batchSet(items);
        const end = performance.now();
        const duration = (end - start) / NUM_OPERATIONS;
        benchmarkResults["batchSet (large values)"] = duration;
        console.log(`batchSet (large values): Average time per operation = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: queryAst (Eq)", async () => {
        // First, populate the database
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const key = `benchmark_query_key_${i}`;
            itemsToSet.push({ key, value: { type: "benchmark", index: i } });
        }
        await db.batchSet(itemsToSet);

        const queryAst: AstNode = { Eq: ["type", "benchmark", "String"] };

        const start = performance.now();
        await db._queryAst(queryAst);
        const end = performance.now();
        const duration = (end - start);
        benchmarkResults["queryAst (Eq)"] = duration;
        console.log(`queryAst (Eq): Total time for query = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: queryAst (Eq, deep nesting)", async () => {
        // First, populate the database
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            let nestedValue: any = { type: "deep_benchmark", index: i };
            let key = `benchmark_deep_query_key_${i}`;
            for (let j = 0; j < DEEP_NESTING_LEVEL; j++) {
                nestedValue = { [`level${j}`]: nestedValue };
            }
            itemsToSet.push({ key, value: nestedValue });
        }
        await db.batchSet(itemsToSet);

        const queryAst: AstNode = { Eq: [`level0.level1.level2.level3.level4.type`, "deep_benchmark", "String"] };

        const start = performance.now();
        await db._queryAst(queryAst);
        const end = performance.now();
        const duration = (end - start);
        benchmarkResults["queryAst (Eq, deep nesting)"] = duration;
        console.log(`queryAst (Eq, deep nesting): Total time for query = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: queryAst (complex)", async () => {
        // First, populate the database
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const key = `benchmark_complex_query_key_${i}`;
            itemsToSet.push({ key, value: { type: "complex", index: i, even: i % 2 === 0, value: i } });
        }
        await db.batchSet(itemsToSet);

        // Complex query with multiple AND/OR conditions
        const queryAst: AstNode = {
            And: [
                { Eq: ["type", "complex", "String"] },
                { Or: [
                        { Gt: ["index", 500, "Number"] },
                        { And: [
                                { Eq: ["even", true, "Bool"] },
                                { Lt: ["value", 200, "Number"] }
                            ]
                        }
                    ]
                }
            ]
        };

        const start = performance.now();
        await db._queryAst(queryAst);
        const end = performance.now();
        const duration = (end - start);
        benchmarkResults["queryAst (complex)"] = duration;
        console.log(`queryAst (complex): Total time for query = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: clearPrefix", async () => {
        // First, populate the database
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.set(`benchmark_prefix/key_${i}`, i);
        }

        const start = performance.now();
        const count = await db.clearPrefix("benchmark_prefix/");
        const end = performance.now();
        const duration = end - start;
        benchmarkResults["clearPrefix"] = duration;
        console.log(`clearPrefix: Total time = ${duration.toFixed(4)}ms, Count = ${count}`);
    });

    it("benchmark: dropDatabase", async () => {
        // First, populate the database
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            await db.set(`benchmark_drop_key_${i}`, i);
        }

        const start = performance.now();
        const count = await db.dropDatabase();
        const end = performance.now();
        const duration = end - start;
        benchmarkResults["dropDatabase"] = duration;
        console.log(`dropDatabase: Total time = ${duration.toFixed(4)}ms, Count = ${count}`);
    });

    it("benchmark: queryRadius", async () => {
        // First, populate the database with some GeoPoints
        const geoPoints = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            geoPoints.push({ key: `geo${i}`, value: { type: "geo", location: { lat: 34.0522 + (i * 0.0001), lon: -118.2437 + (i * 0.0001) } } });
        }
        await db.batchSet(geoPoints);

        const start = performance.now();
        await db.queryRadius({ field: "location", lat: 34.0522, lon: -118.2437, radius: 100 });
        const end = performance.now();
        const duration = end - start;
        benchmarkResults["queryRadius"] = duration;
        console.log(`queryRadius: Total time = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: queryBox", async () => {
        // First, populate the database with some GeoPoints
        const geoPoints = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            geoPoints.push({ key: `geo${i}`, value: { type: "geo", location: { lat: 34.0522 + (i * 0.0001), lon: -118.2437 + (i * 0.0001) } } });
        }
        await db.batchSet(geoPoints);

        const start = performance.now();
        await db.queryBox({ field: "location", min_lat: 34.0521, min_lon: -118.2438, max_lat: 34.0525, max_lon: -118.2436 });
        const end = performance.now();
        const duration = end - start;
        benchmarkResults["queryBox"] = duration;
        console.log(`queryBox: Total time = ${duration.toFixed(4)}ms`);
    });

    it("benchmark: queryAnd", async () => {
        // Populate the database
        const itemsToSet: BatchSetItem[] = [];
        for (let i = 0; i < NUM_OPERATIONS; i++) {
            const key = `benchmark_and_key_${i}`;
            itemsToSet.push({ key, value: { type: "and_test", index: i, even: i % 2 === 0, value: i } });
        }
        await db.batchSet(itemsToSet);

        const start = performance.now();
        await db.queryAnd([["type", "===", "and_test"], ["even", "===", "true"]]);
        const end = performance.now();
        const duration = end - start;
        benchmarkResults["queryAnd"] = duration;
        console.log(`queryAnd: Total time = ${duration.toFixed(4)}ms`);
    });
});
