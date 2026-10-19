import { describe, expect, it } from "vite-plus/test";

import { groupGitHubAccounts, nextGitHubHosts, nextGitHubRules } from "./githubAccounts.ts";

describe("groupGitHubAccounts", () => {
  it("groups logins by host, keeping broken and environment logins out of the picker", () => {
    expect(
      groupGitHubAccounts([
        { host: "github.com", account: "personal", active: true, authenticated: true },
        { host: "github.com", account: "work", active: false, authenticated: true },
        { host: "github.com", account: "old", active: false, authenticated: false, error: "bad" },
        {
          host: "github.com",
          account: "bot",
          active: false,
          authenticated: true,
          environmentVariable: "GH_TOKEN",
        },
        { host: "ghe.acme.test", account: "jm", active: false, authenticated: true },
      ]),
    ).toEqual([
      {
        host: "github.com",
        activeAccount: "personal",
        selectable: ["personal", "work"],
        broken: [
          { host: "github.com", account: "old", active: false, authenticated: false, error: "bad" },
        ],
        environmentVariable: "GH_TOKEN",
      },
      {
        host: "ghe.acme.test",
        activeAccount: "jm",
        selectable: ["jm"],
        broken: [],
        environmentVariable: null,
      },
    ]);
  });
});

describe("nextGitHubHosts", () => {
  it("pins an account, keeps other hosts, and drops a host back on gh's defaults", () => {
    const other = { "ghe.acme.test": { enabled: false } };
    const pinned = nextGitHubHosts(other, "github.com", { account: "work" });
    expect(pinned).toEqual({ ...other, "github.com": { enabled: true, account: "work" } });

    const disabled = nextGitHubHosts(pinned, "github.com", { enabled: false });
    expect(disabled["github.com"]).toEqual({ enabled: false, account: "work" });

    expect(nextGitHubHosts(disabled, "github.com", { enabled: true, account: null })).toEqual(
      other,
    );
  });
});

describe("owner rules", () => {
  it("keeps a repository's rule beside its owner's, and drops the host once both go", () => {
    const owners = nextGitHubRules(undefined, "acme", "work");
    const repositories = nextGitHubRules(undefined, "Acme/Handbook", "signed-out");
    const hosts = nextGitHubHosts(nextGitHubHosts({}, "github.com", { owners }), "github.com", {
      repositories,
    });
    expect(hosts).toEqual({
      "github.com": {
        enabled: true,
        owners: { acme: "work" },
        repositories: { "acme/handbook": "signed-out" },
      },
    });

    const withoutOwner = nextGitHubHosts(hosts, "github.com", {
      owners: nextGitHubRules(owners, "acme", null),
    });
    expect(withoutOwner["github.com"]?.repositories).toEqual({ "acme/handbook": "signed-out" });
    expect(
      nextGitHubHosts(withoutOwner, "github.com", {
        repositories: nextGitHubRules(repositories, "acme/handbook", null),
      }),
    ).toEqual({});
  });
});
