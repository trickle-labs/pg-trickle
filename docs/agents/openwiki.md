# OpenWiki with ChatGPT Pro

This repository uses [LangChain OpenWiki's Codex integration](https://github.com/langchain-ai/openwiki#quick-start).
Codex supplies its authenticated model session and repository tools; OpenWiki
supplies local search, source-backed Claims, and resumable wiki maintenance.
[ChatGPT sign-in uses subscription access](https://learn.chatgpt.com/docs/auth#openai-authentication),
so this path uses your existing Pro plan's Codex allowance without an OpenAI API
key or a separate OpenWiki OAuth token.

## Setup

OpenWiki 0.7.0 was used to install the project integration. It requires Node.js
22.22.0 or newer. On another machine, install that version and verify ChatGPT
authentication:

```sh
npm install -g openwiki@0.7.0
codex login status
```

If Codex is not signed in with ChatGPT, run `codex login` and use the account
with the Pro subscription. Restart Codex in this repository so it loads the
project skill and MCP server. Project configuration requires a
[trusted repository](https://learn.chatgpt.com/docs/config-file/config-basic).

The checked-in configuration is:

- `.codex/config.toml`: starts `openwiki mcp --host codex` locally over stdio.
- `.agents/skills/openwiki/`: the upstream skill and installation receipt.
- `.openwikiignore`: excludes private state, dependencies, and generated output.
- `openwiki/INSTRUCTIONS.md`: the project's documentation scope and priorities.

Keep the host's selected model. For this codebase, high reasoning is a useful
starting point for initial architecture research and correctness-sensitive
updates; medium is a reasonable starting point for routine edits. These are
workload recommendations, not benchmark results. The integration inherits the
Codex session's model and effort; native CLI `OPENWIKI_MODEL_ID`,
`OPENWIKI_REASONING_EFFORT`, and page-concurrency settings do not configure it.
Pages run sequentially and progress is saved per page.

## Use

After restarting Codex, ask:

> Use $openwiki to initialize this repository's OpenWiki from current source
> and tests, following openwiki/INSTRUCTIONS.md.

For subsequent maintenance:

> Use $openwiki to update this repository's OpenWiki from the current source
> and tests.

For a focused question:

> Search this repository's OpenWiki for CDC transaction boundaries, then read
> the relevant sections and verify consequential details against current source.

Review generated Markdown and Claims before committing. OpenWiki maintains its
own marked block in root agent instructions. To explore the generated wiki:

```sh
openwiki visualize openwiki
```

Use the Codex requests above for generation with this configuration. Bare
`openwiki --init` or `openwiki --update` uses the standalone provider setup,
which is separate from the Codex session. Reinitialization replaces generated
pages; use update after the first completed generation.

Changes limited to `AGENTS.md` files or the OpenWiki pages, skill, ignore file,
guide, and current Codex MCP config skip automatic push/PR workflows. Mixed
changes still select the relevant checks; scheduled and manual runs remain
available.

No OpenWiki schedule or external connector is configured. Generation consumes the
same Codex allowance as other work on your Pro plan; it does not provide an
additional allowance. Avoid storing credentials in the repository. In the host
integration, the agent must honor `.openwikiignore` with its native tools; the
ignore file is not a replacement for the host's filesystem permissions.
