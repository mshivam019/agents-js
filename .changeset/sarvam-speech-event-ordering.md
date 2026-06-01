---
"@livekit/agents-plugin-sarvam": patch
---

fix: ensure speech-event ordering in Sarvam STT WebSocket stream

The Sarvam WebSocket can deliver END_SPEECH signals before the corresponding
FINAL_TRANSCRIPT arrives, causing premature turn closure in the agent pipeline.
This change holds END_OF_SPEECH events until a transcript is received, matching
the Python SDK behavior.
