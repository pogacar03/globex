import assert from "node:assert/strict";
import test from "node:test";

import {
  approveAndExecuteApproval,
  executeApprovedApproval,
  fetchSessionApprovals,
} from "../src/approvalWorkflow.ts";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function record(state: string, extras: Record<string, unknown> = {}) {
  return {
    approval_id: "approval-1",
    shopping_session_id: "tenant-session-1",
    state,
    tool: "create_order_tool",
    arguments: { sku: "SKU-1", quantity: 1 },
    ...extras,
  };
}

test("only reports completion after execute receipt and persisted completed state", async () => {
  const calls: Array<{ url: string; method: string; authorization: string | null }> = [];
  const receipt = { order_id: "order-42", status: "created" };
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    calls.push({ url, method, authorization: new Headers(init?.headers).get("Authorization") });
    if (url.endsWith("/approve")) return jsonResponse(record("approved"));
    if (url.endsWith("/execute")) return jsonResponse({ state: "completed", result: receipt });
    return jsonResponse(record("completed", { result: receipt }));
  };

  const resolution = await approveAndExecuteApproval(
    "http://api.test", "fixture-secret", "approval-1", fetcher,
  );

  assert.equal(resolution.completed, true);
  assert.equal(resolution.record?.state, "completed");
  assert.deepEqual(resolution.record?.result, receipt);
  assert.deepEqual(calls.map(({ method, url }) => [method, new URL(url).pathname]), [
    ["POST", "/commerce/approvals/approval-1/approve"],
    ["POST", "/commerce/approvals/approval-1/execute"],
    ["GET", "/commerce/approvals/approval-1"],
  ]);
  assert.ok(calls.every((call) => call.authorization === "Bearer fixture-secret"));
});

for (const state of ["approved", "executing", "uncertain"] as const) {
  test(`does not report completion when executor or persisted record is ${state}`, async () => {
    const fetcher: typeof fetch = async (input, init) => {
      const url = String(input);
      if (url.endsWith("/approve")) return jsonResponse(record("approved"));
      if (url.endsWith("/execute")) return jsonResponse({ state });
      return jsonResponse(record(state));
    };

    const resolution = await approveAndExecuteApproval(
      "http://api.test", "fixture-secret", "approval-1", fetcher,
    );

    assert.equal(resolution.completed, false);
    assert.equal(resolution.record?.state, state);
  });
}

test("does not claim completion if execute response is not confirmed by persisted state", async () => {
  const receipt = { order_id: "order-42" };
  const fetcher: typeof fetch = async (input) => {
    const url = String(input);
    if (url.endsWith("/approve")) return jsonResponse(record("approved"));
    if (url.endsWith("/execute")) return jsonResponse({ state: "completed", result: receipt });
    return jsonResponse(record("executing"));
  };

  const resolution = await approveAndExecuteApproval(
    "http://api.test", "fixture-secret", "approval-1", fetcher,
  );

  assert.equal(resolution.completed, false);
  assert.equal(resolution.record?.state, "executing");
});

test("after an execute error, checks persisted state without retrying the write", async () => {
  const methods: string[] = [];
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    methods.push(method);
    if (url.endsWith("/approve")) return jsonResponse(record("approved"));
    if (url.endsWith("/execute")) return jsonResponse({ detail: "execution outcome unclear" }, 503);
    return jsonResponse(record("uncertain"));
  };

  const resolution = await approveAndExecuteApproval(
    "http://api.test", "fixture-secret", "approval-1", fetcher,
  );

  assert.equal(resolution.completed, false);
  assert.equal(resolution.record?.state, "uncertain");
  assert.deepEqual(methods, ["POST", "POST", "GET"]);
});

test("an ambiguous approval response checks status and does not start execution", async () => {
  const methods: string[] = [];
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    methods.push(method);
    if (url.endsWith("/approve")) return jsonResponse({ detail: "already approved" }, 409);
    return jsonResponse(record("approved"));
  };

  const resolution = await approveAndExecuteApproval(
    "http://api.test", "fixture-secret", "approval-1", fetcher,
  );

  assert.equal(resolution.completed, false);
  assert.equal(resolution.record?.state, "approved");
  assert.deepEqual(methods, ["POST", "GET"]);
});

test("loads persisted approvals from the authenticated session and keeps server scope", async () => {
  let requestedUrl = "";
  let authorization: string | null = null;
  const fetcher: typeof fetch = async (input, init) => {
    requestedUrl = String(input);
    authorization = new Headers(init?.headers).get("Authorization");
    return jsonResponse({ approvals: [record("pending")] });
  };

  const approvals = await fetchSessionApprovals(
    "http://api.test", "fixture-secret", "raw-session", fetcher,
  );

  assert.equal(new URL(requestedUrl).pathname, "/commerce/approvals");
  assert.equal(new URL(requestedUrl).searchParams.get("shopping_session_id"), "raw-session");
  assert.equal(authorization, "Bearer fixture-secret");
  assert.equal(approvals[0]?.shopping_session_id, "tenant-session-1");
});

test("continues an approved action after reload without approving again or asking an LLM", async () => {
  const calls: Array<[string, string]> = [];
  let reads = 0;
  const receipt = { order_id: "order-42" };
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    calls.push([init?.method ?? "GET", new URL(url).pathname]);
    assert.equal(init?.body, undefined, "execution must use stored arguments");
    if (url.endsWith("/execute")) return jsonResponse({ state: "completed", result: receipt });
    return jsonResponse(++reads === 1 ? record("approved") : record("completed", { result: receipt }));
  };
  const resolution = await executeApprovedApproval("http://api.test", "fixture-secret", "approval-1", fetcher);
  assert.equal(resolution.completed, true);
  assert.deepEqual(calls, [["GET", "/commerce/approvals/approval-1"],
    ["POST", "/commerce/approvals/approval-1/execute"], ["GET", "/commerce/approvals/approval-1"]]);
});

for (const state of ["pending", "executing", "uncertain"] as const) {
  test(`does not resume a ${state} action automatically`, async () => {
    let calls = 0;
    const fetcher: typeof fetch = async (_input, init) => {
      ++calls;
      assert.equal(init?.method ?? "GET", "GET");
      return jsonResponse(record(state));
    };
    const resolution = await executeApprovedApproval("http://api.test", "fixture-secret", "approval-1", fetcher);
    assert.equal(resolution.completed, false);
    assert.equal(calls, 1);
  });
}
