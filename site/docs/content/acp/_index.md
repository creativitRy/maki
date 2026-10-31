+++
title = "ACP"
weight = 22
[extra]
group = "Guides"
+++

# ACP (Agent Client Protocol)

Run Maki inside your editor. `maki acp` starts an [ACP](https://agentclientprotocol.com/) server over stdio, so any ACP-capable editor (like [Zed](https://zed.dev/)) can drive Maki as its coding agent.

```bash
maki acp
```

## Zed setup

Add Maki as a custom agent in Zed's `settings.json`:

```json
"agent_servers": {
  "Maki": {
    "default_config_options": {
      "model": "deepseek/deepseek-flash"
    },
    "type": "custom",
    "command": "maki",
    "args": ["acp"],
    "env": {}
  }
}
```

The `model` value is a `provider/model-id` spec, same format as `maki --model`.

## What works

- **Sessions persist.** Loading a session replays the full conversation in the editor, so you can resume where you left off.
- **Resume picker.** Editors can list this project's stored sessions, newest first, to resume an old transcript instead of starting a blank one.
- **Model switching.** Pick a model from the editor's dropdown, mid-session. All configured providers show up. Providers that list their models over the wire (OpenRouter and friends) are discovered in the background, so the dropdown keeps filling up for a moment after the session starts, one provider at a time.
- **Modes.** Switch between build (full access) and plan (plan-file writes only) from the editor.
- **Permissions.** Tool permission prompts appear in the editor: allow or reject, once or always.
- **Questions.** The `question` tool becomes a native form in the editor (ACP elicitation). If the client does not support elicitation, the tool is dropped and the model asks in plain text.
- **Live tool calls.** Tool progress streams as it happens, including sub-agents and batched calls.
- **Images and context.** Prompts can include images and editor-attached files.

Authentication, providers, and permissions come from your normal Maki config. Set up [providers](/docs/providers/) first and ACP sessions just work.

The editor picks each session's working directory, and that folder's own
[trust](/docs/folder-trust/) decides whether its `.maki` config loads. ACP never
asks, so trust a project with `maki trust add` in it, or start the server with
`maki --trust acp`.

```bash
maki acp
maki acp -m anthropic/claude-sonnet-4-6
maki acp --yolo
maki acp --multiplex
maki --no-jit acp
```

`maki acp` only takes `-m` / `--model`, `--yolo`, and `--multiplex` as subcommand flags. Global flags like `--no-jit` must come before the subcommand (`maki --no-jit acp`, not `maki acp --no-jit`).

Plan mode in ACP uses the same state-directory plan files as the TUI (`…/plans/<slug>.md`), not the SDK's `./plan.md`.

## Background tasks

`background = true` and `background_on_timeout` move work to the background as in the TUI. No turn starts on its own, so results arrive with the next prompt. Closing the ACP session stops the work.

Moves are on unless you pass `--background-timeout 0`, which turns them off. ACP only checks whether they are on, since results arrive with the next prompt. It is a global flag: `maki --background-timeout 0 acp`. See [Background Tasks](/docs/background-tasks/#headless-sdk-and-acp).

SIGINT or SIGTERM closes the session like the end of input, then exits with 130 or 143.

## Multiple sessions

Plain `maki acp` runs one session at a time, so a new session replaces the old one. Editors like JetBrains Air open many chats in one agent process. Start the server with `--multiplex` to give each session its own `maki acp` child:

```bash
maki acp --multiplex
```

The shim answers `initialize` and starts a child on `session/new` or `session/load`, in the working directory the editor sends for that session. The child starts there, so the project's config, instructions, Lua plugins, and tool paths apply from the first prompt. Each message about a session goes to its own child.

```json
"agent_servers": {
  "Maki": {
    "type": "custom",
    "command": "maki",
    "args": ["acp", "--multiplex"],
    "env": {}
  }
}
```

The shim keeps its own working directory and uses it only to start children. Without `--multiplex`, a client that launches Maki outside the project (Air does) runs every session in that launch directory.

### Lifetime

A child lives until its session is closed with `session/close` or the shim stops. Closing a chat sends `session/close`. The child exits, and the stored session stays loadable. Air stops an idle shim after two minutes, which shuts down its children too.

An editor that neither closes sessions nor stops the agent keeps one child per chat it ever opened. Each child loads the Lua host, the config, and its own MCP servers, so memory grows with the number of open chats.

### MCP servers

Servers injected by the client reach each child unchanged. Every child starts its own copy, the same way plain `maki acp` does. Sharing a single MCP connection across sessions is not implemented yet.
