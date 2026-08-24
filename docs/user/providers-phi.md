# Phi

Phi support is in early access. T3 Code starts the `phi` CLI in RPC mode and falls back to `pi`
when `phi` is not installed. Phi keeps its agent configuration under `~/.pi/agent` by default;
you can set a different binary or agent config directory in the provider settings.

The current integration supports:

- starting a new conversation or resuming its Phi session identity
- sending text prompts and streaming assistant text
- interrupting and stopping sessions
- selecting a model when the session starts

Tool activity is not shown yet. Interactive permissions and questions from Phi extensions are not
available, and attachments, durable history reconstruction, rollback, diff synthesis, and feedback
upload are not supported. Start a new conversation to change models.
