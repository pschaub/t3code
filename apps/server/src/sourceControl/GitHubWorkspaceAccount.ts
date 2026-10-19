import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";

import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";

import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";

const workspaceGitHubRepository = Effect.fn("GitHubWorkspaceAccount.repository")(function* (
  cwd: string,
) {
  const resolver = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const repository = yield* resolver.resolveGitHubRepository(cwd);
  return repository === null
    ? null
    : { host: repository.host, repository: `${repository.owner}/${repository.name}` };
});

export const workspaceGitHubCredential = Effect.fn("GitHubWorkspaceAccount.credential")(function* (
  cwd: string,
) {
  const workspace = yield* workspaceGitHubRepository(cwd);
  if (workspace === null) return null;
  const credentials = yield* GitHubCredentials.GitHubCredentials;
  return yield* credentials
    .get(workspace.host, workspace.repository)
    .pipe(Effect.orElseSucceed(() => null));
});

export const agentGitHubEnvironment = Effect.fn("GitHubWorkspaceAccount.agentEnvironment")(
  function* (cwd: string) {
    const none: Readonly<Record<string, Redacted.Redacted<string>>> = {};
    const workspace = yield* workspaceGitHubRepository(cwd);
    // gh sends enterprise tokens to any non-public host a command names.
    // Keep enterprise credentials in gh's host-scoped store instead.
    if (workspace === null || !GitHubCredentials.isGitHubDotCom(workspace.host)) return none;
    const credentials = yield* GitHubCredentials.GitHubCredentials;
    if ((yield* credentials.accountFor(workspace.host, workspace.repository)) === undefined) {
      return none;
    }
    const credential = yield* credentials
      .get(workspace.host, workspace.repository)
      .pipe(Effect.orElseSucceed(() => null));
    return credential?.account === undefined ? none : { GH_TOKEN: credential.token };
  },
);
