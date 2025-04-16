import { serve } from "bun";
import jwt from "jsonwebtoken";
import { request } from "undici"; // Import undici request

const JWT_SECRET = process.env.JWT_SECRET || "your-very-secret-key-change-me";
const PORT = parseInt(process.env.PORT || "8080", 10);
// *** Use port 8989 to match working curl test and server config ***
const RUST_SERVER_URL = process.env.RUST_SERVER_URL || "http://localhost:8989";
const FORWARD_TIMEOUT_MS = 10000;

// --- Helper Function ---
function verifyToken(token: string | undefined | null): { valid: boolean; payload?: any; error?: string } {
  if (!token) {
    return { valid: false, error: "No token provided" };
  }
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    return { valid: true, payload };
  } catch (err: any) {
    return { valid: false, error: `Invalid token: ${err.message}` };
  }
}

// --- Forward Request Helper using Undici ---
async function forwardRequest(
    rustPath: string,
    method: string,
    body?: any,
    contentType: string | null = "application/json"
): Promise<Response> {
    const targetUrl = `${RUST_SERVER_URL}${rustPath}`;
    console.log(`[Gateway] Preparing to forward ${method} request to ${targetUrl} using undici`);

    const headers: Record<string, string> = {}; // Undici uses plain objects for headers
    let reqBody: string | Buffer | undefined = undefined;

    if (body) {
        if (contentType === "application/json") {
            headers["content-type"] = "application/json"; // Lowercase header names common
            try {
                reqBody = (typeof body === 'string') ? body : JSON.stringify(body);
                 console.log(`[Gateway] Forwarding JSON body: ${reqBody.substring(0, 100)}...`);
            } catch (e) {
                 console.error("[Gateway] Failed to stringify body for forwarding:", e);
                 return new Response(JSON.stringify({ error: "Internal gateway error stringifying body" }), { status: 500 });
            }
        } else if (contentType) {
             headers["content-type"] = contentType;
             // Assuming body is already in a suitable format (string/Buffer) if not JSON
             reqBody = body as string | Buffer;
             console.log(`[Gateway] Forwarding body with Content-Type: ${contentType}`);
        } else {
         console.log("[Gateway] Forwarding request with no body.");
        }
    }

    try {
        console.log(`[Gateway] Sending undici request to ${targetUrl} with ${FORWARD_TIMEOUT_MS / 1000}s timeout...`);

        const requestPromise = request(targetUrl, {
            method: method as any, // Cast method string
            headers: headers,
            body: reqBody,
            // Undici has built-in timeout options, but let's keep Promise.race for consistency for now
            // signal: AbortSignal.timeout(FORWARD_TIMEOUT_MS) // Alternative timeout mechanism
        });

        const timeoutPromise = new Promise<never>((_, reject) => // Timeout rejects with Error
            setTimeout(() => reject(new Error(`[Gateway] Timeout waiting for response from ${targetUrl}`)), FORWARD_TIMEOUT_MS)
        );

        // Race the undici request against the timeout
        const { statusCode, headers: responseHeadersObj, body: responseBodyStream } = await Promise.race([requestPromise, timeoutPromise]);

        console.log(`[Gateway] Received response ${statusCode} from ${targetUrl}`);

        // Convert undici headers (object) to Headers object
        const responseHeaders = new Headers();
        for (const [key, value] of Object.entries(responseHeadersObj)) {
             if (value) { // Handle potential undefined/null values
                 if (Array.isArray(value)) {
                     value.forEach(v => responseHeaders.append(key, v));
                 } else {
                     responseHeaders.append(key, value);
                 }
             }
        }

        // Consume the undici body stream into an ArrayBuffer
        const chunks: Buffer[] = [];
        for await (const chunk of responseBodyStream) {
            chunks.push(chunk);
        }
        const responseBody = Buffer.concat(chunks).buffer; // Get ArrayBuffer

        console.log(`[Gateway] Read ${responseBody.byteLength} bytes from backend response.`);
        console.log(`[Gateway] Returning response with status ${statusCode} to client.`);

        return new Response(responseBody, {
            status: statusCode,
            headers: responseHeaders,
        });

    } catch (err: any) {
        console.error(`[Gateway] Error during undici request/timeout to ${targetUrl}:`, err);
        if (err.message?.includes("Timeout waiting")) {
             return new Response(JSON.stringify({ error: "Database service timeout" }), { status: 504 });
        }
        // Undici errors might have different codes (e.g., ECONNREFUSED)
        if (err.code === 'ECONNREFUSED') {
             return new Response(JSON.stringify({ error: "Database service unavailable" }), { status: 503 });
        }
        // Add more specific undici error handling if needed
        return new Response(JSON.stringify({ error: "Internal gateway error during fetch" }), { status: 500 });
    }
}


// --- Start the Bun server (Gateway) ---
console.log(`[Gateway] Starting Bun gateway server on port ${PORT}...`);
console.log(`[Gateway] Forwarding requests to Rust backend at ${RUST_SERVER_URL}`);

serve({
  async fetch(req) {
    const url = new URL(req.url);
    const tokenHeader = req.headers.get("Authorization");
    const token = tokenHeader?.startsWith("Bearer ") ? tokenHeader.substring(7) : null;

    // --- Authentication ---
    const auth = verifyToken(token);
    if (!auth.valid) {
      console.log(`[Gateway] Unauthorized request to ${url.pathname}: ${auth.error}`);
      return new Response(auth.error || "Unauthorized", { status: 401 });
    }

    // --- Request Parsing and Forwarding ---
    let body: any;
    let bodyText: string | null = null;
    const contentTypeHeader = req.headers.get("content-type");
    const isJson = contentTypeHeader?.includes("application/json");

    if (req.method === "POST" || req.method === "PUT") {
         try {
             bodyText = await req.text();
             if (isJson && bodyText) {
                 body = JSON.parse(bodyText);
             } else {
                 body = bodyText;
             }
         } catch (e) {
              console.error(`[Gateway] Invalid request body for ${req.method} ${url.pathname}:`, e);
             return new Response("Invalid request body", { status: 400 });
         }
    }

    // --- API Route Mapping and Forwarding ---
    // Use the new forwardRequest function implicitly
    switch (url.pathname) {
        case "/set":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            // Auto serialize to JSON if it's not a string
            const setBody = typeof body === 'string' ? body : JSON.stringify(body);
            return forwardRequest("/set", "POST", setBody, "application/json");

        case "/get":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            const getResponse = await forwardRequest("/get", "POST", body, "application/json");
            if (!getResponse.ok) return getResponse; // Propagate errors
            try {
                const getText = await getResponse.text();
                // Auto parse JSON if it's a valid JSON string
                const parsed = JSON.parse(getText);
                return new Response(JSON.stringify(parsed), { // Re-stringify to ensure correct content type
                    status: getResponse.status,
                    headers: getResponse.headers,
                });
            } catch (e) {
                console.error("[Gateway] Failed to parse JSON from backend:", e);
                return getResponse; // Return original response if parsing fails
            }
        case "/query/field":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            return forwardRequest("/query/field", "POST", body, "application/json");

        case "/delete":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            return forwardRequest("/delete", "POST", body, "application/json");

        case "/query/radius":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            return forwardRequest("/query/radius", "POST", body, "application/json");

        case "/query/box":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            return forwardRequest("/query/box", "POST", body, "application/json");

        case "/export":
            if (req.method !== "GET") return new Response("Method Not Allowed", { status: 405 });
            const exportResponse = await forwardRequest("/export", "GET", null, null);
            if (!exportResponse.ok) return exportResponse; // Propagate errors
            try {
                const exportText = await exportResponse.text();
                 return new Response(JSON.stringify(JSON.parse(exportText)), { // Re-stringify to ensure correct content type
                    status: exportResponse.status,
                    headers: exportResponse.headers,
                });
            } catch (e) {
                console.error("[Gateway] Failed to parse JSON from backend:", e);
                return exportResponse; // Return original response if parsing fails
            }

        case "/import":
            if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
            let importBodyString: string;
             if (typeof body === 'string') {
                 try {
                     JSON.parse(body);
                     importBodyString = body;
                 } catch (e) {
                      console.error(`[Gateway] Invalid JSON in import body string: ${body.substring(0,100)}...`);
                      return new Response("Import body must be a valid JSON array string", { status: 400 });
                 }
             } else if (Array.isArray(body)) {
                 importBodyString = JSON.stringify(body);
             } else {
                  console.error(`[Gateway] Invalid import body type: ${typeof body}`);
                 return new Response("Import requires a JSON array body", { status: 400 });
             }
            return forwardRequest("/import", "POST", importBodyString, "application/json");

        default:
            console.log(`[Gateway] Not Found: ${req.method} ${url.pathname}`);
            return new Response("Not Found", { status: 404 });
    }
  },
  port: PORT,
  error(error: Error) {
      console.error("[Gateway] Unhandled Bun server error:", error);
      return new Response("Internal Server Error", { status: 500 });
  }
});