import { assert, describe, it } from "@effect/vitest";

import { openCodeToolTurnItem } from "./toolItems.ts";

type ToolItemBase = Parameters<typeof openCodeToolTurnItem>[0];

const base = { status: "completed", title: "Tool" } as ToolItemBase;

const toolItem = (name: string, input: Record<string, unknown>) =>
  openCodeToolTurnItem(base, { name, input, output: "tool output", completedMetadata: undefined });

describe("openCodeToolTurnItem", () => {
  it.each([
    ["billing_credit_balance", { account: "acme" }],
    ["github_dispatch_workflow", { workflow: "ci.yml" }],
    ["linear_edit_issue", { id: "T3-1", title: "New title" }],
    ["notion_write_page", { pageId: "abc", content: "Notes" }],
  ])("keeps the output of %s, which edits no file", (name, input) => {
    const item = toolItem(name, input);
    assert.equal(item.type, "dynamic_tool");
    assert.equal(item.type === "dynamic_tool" ? item.output : undefined, "tool output");
  });

  it.each([
    ["edit", {}],
    ["write", {}],
    ["apply_patch", { patchText: "*** Begin Patch" }],
    ["filesystem_write_file", { path: "/workspace/notes.md" }],
  ])("shows %s as a file change", (name, input) => {
    assert.equal(toolItem(name, input).type, "file_change");
  });
});
