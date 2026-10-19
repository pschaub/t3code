import {
  pullRequestHostOf,
  type RepositoryIdentity,
  SourceControlProviderKind,
  type SourceControlProviderAuth,
} from "@t3tools/contracts";
import {
  gitHubAccountsFor,
  type GitHubSettings,
} from "@t3tools/source-control-github/client/definition";

const GITHUB = SourceControlProviderKind.make("github");

export type GitHubDiscoveredAccount = NonNullable<SourceControlProviderAuth["accounts"]>[number];

export interface GitHubHostGroup {
  readonly host: string;
  readonly activeAccount: string | null;
  readonly selectable: ReadonlyArray<string>;
  readonly broken: ReadonlyArray<GitHubDiscoveredAccount>;
  readonly environmentVariable: string | null;
}

export function groupGitHubAccounts(
  accounts: ReadonlyArray<GitHubDiscoveredAccount>,
  choices: GitHubSettings["hosts"] = {},
): ReadonlyArray<GitHubHostGroup> {
  const hosts = [...new Set([...accounts.map((entry) => entry.host), ...Object.keys(choices)])];
  return hosts.map((host) => {
    const entries = accounts.filter((entry) => entry.host === host);
    const stored = entries.filter((entry) => entry.environmentVariable === undefined);
    const selectable = stored.filter((entry) => entry.authenticated);
    return {
      host,
      activeAccount: (selectable.find((entry) => entry.active) ?? selectable[0])?.account ?? null,
      selectable: selectable.map((entry) => entry.account),
      broken: stored.filter((entry) => !entry.authenticated),
      environmentVariable:
        entries.find((entry) => entry.environmentVariable)?.environmentVariable ?? null,
    };
  });
}

type AccountRules = Readonly<Record<string, string>>;

const hasRules = (rules: AccountRules | undefined): rules is AccountRules =>
  rules !== undefined && Object.keys(rules).length > 0;

export function nextGitHubHosts(
  current: GitHubSettings["hosts"],
  host: string,
  change: {
    readonly enabled?: boolean;
    readonly account?: string | null;
    readonly owners?: AccountRules;
    readonly repositories?: AccountRules;
  },
): GitHubSettings["hosts"] {
  const previous = current[host];
  const enabled = change.enabled ?? previous?.enabled ?? true;
  const account = change.account === undefined ? previous?.account : (change.account ?? undefined);
  const owners = change.owners ?? previous?.owners;
  const repositories = change.repositories ?? previous?.repositories;
  const { [host]: _replaced, ...rest } = current;
  if (enabled && account === undefined && !hasRules(owners) && !hasRules(repositories)) {
    return rest;
  }
  return {
    ...rest,
    [host]: {
      enabled,
      ...(account === undefined ? {} : { account }),
      ...(hasRules(owners) ? { owners } : {}),
      ...(hasRules(repositories) ? { repositories } : {}),
    },
  };
}

export function nextGitHubRules(
  current: AccountRules | undefined,
  key: string,
  account: string | null,
): AccountRules {
  const normalized = key.trim().toLowerCase();
  const { [normalized]: _replaced, ...rest } = current ?? {};
  return account === null ? rest : { ...rest, [normalized]: account };
}

export function gitHubRepositoryOf(
  identity: RepositoryIdentity | null | undefined,
): { readonly host: string; readonly repository: string } | null {
  const owner = identity?.owner?.trim().toLowerCase();
  const name = identity?.name?.trim().toLowerCase();
  if (
    !identity ||
    (identity.provider !== undefined && identity.provider !== GITHUB) ||
    !owner ||
    !name
  )
    return null;
  return { host: pullRequestHostOf(identity, GITHUB), repository: `${owner}/${name}` };
}

export function gitHubOwnersOnHost(
  host: string,
  identities: ReadonlyArray<RepositoryIdentity | null | undefined>,
  rules: AccountRules | undefined,
): ReadonlyArray<string> {
  const owners = new Set(Object.keys(rules ?? {}));
  for (const identity of identities) {
    if (identity?.provider !== undefined && identity.provider !== GITHUB) continue;
    const owner = identity?.owner?.trim().toLowerCase();
    if (owner && pullRequestHostOf(identity, GITHUB) === host) owners.add(owner);
  }
  return [...owners].sort((left, right) => left.localeCompare(right));
}

export function gitHubDefaultAccount(
  group: GitHubHostGroup,
  choice: GitHubSettings["hosts"][string] | undefined,
  repository?: string,
): string | null {
  return (
    gitHubAccountsFor(choice, repository).find((account) => group.selectable.includes(account)) ??
    group.activeAccount
  );
}
