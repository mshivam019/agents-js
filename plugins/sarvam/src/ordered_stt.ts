// SPDX-FileCopyrightText: 2026 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import { type APIConnectOptions, type AudioBuffer, stt } from '@livekit/agents';
import { STT, type STTOptions } from './stt.js';

const SARVAM_SAMPLE_RATE = 16000;

/**
 * A Sarvam STT wrapper that ensures speech-event ordering.
 *
 * @remarks
 * The upstream Sarvam WebSocket may emit `END_OF_SPEECH` before the corresponding
 * `FINAL_TRANSCRIPT` has arrived.  This wrapper holds the end-of-speech event until
 * a final transcript is received, preventing premature turn closure in the agent
 * pipeline.
 *
 * Usage is identical to the base {@link STT}:
 *
 * ```typescript
 * import { OrderedSTT } from '@livekit/agents-plugin-sarvam';
 *
 * const stt = new OrderedSTT({
 *   model: 'saaras:v3',
 *   languageCode: 'en-IN',
 * });
 * ```
 */
export class OrderedSTT extends stt.STT {
  readonly label = 'sarvam.OrderedSTT';
  #inner: STT;

  constructor(opts: Partial<STTOptions> = {}) {
    const inner = new STT(opts);
    super(inner.capabilities);
    this.#inner = inner;
  }

  override get model(): string {
    return this.#inner.model;
  }

  override get provider(): string {
    return this.#inner.provider;
  }

  updateOptions(opts: Partial<STTOptions>): void {
    this.#inner.updateOptions(opts);
  }

  protected override _recognize(
    frame: AudioBuffer,
    abortSignal?: AbortSignal,
  ): Promise<stt.SpeechEvent> {
    return this.#inner.recognize(frame, abortSignal);
  }

  override stream(options?: { connOptions?: APIConnectOptions }): stt.SpeechStream {
    return new OrderedSpeechStream(this, this.#inner.stream(options), options?.connOptions);
  }

  override close(): Promise<void> {
    return this.#inner.close();
  }
}

class OrderedSpeechStream extends stt.SpeechStream {
  readonly label = 'sarvam.OrderedSpeechStream';
  #inner: stt.SpeechStream;
  #pendingEndOfSpeech: stt.SpeechEvent | undefined;
  #hasTranscriptForCurrentSpeech = false;

  constructor(sttProvider: stt.STT, inner: stt.SpeechStream, connOptions?: APIConnectOptions) {
    super(sttProvider, SARVAM_SAMPLE_RATE, connOptions);
    this.#inner = inner;
  }

  protected async run(): Promise<void> {
    const inputTask = this.#forwardInput();

    try {
      for await (const event of this.#inner) {
        this.#handleInnerEvent(event);
      }
    } finally {
      this.#inner.close();
      if (!this.input.closed) {
        this.input.close();
      }
      await inputTask.catch(() => undefined);
    }
  }

  async #forwardInput(): Promise<void> {
    try {
      for await (const input of this.input) {
        if (input === OrderedSpeechStream.FLUSH_SENTINEL) {
          this.#inner.flush();
        } else {
          this.#inner.pushFrame(input);
        }
      }

      this.#inner.endInput();
    } catch {
      // The outer stream may close while the inner stream is reconnecting or shutting down.
    }
  }

  #handleInnerEvent(event: stt.SpeechEvent): void {
    switch (event.type) {
      case stt.SpeechEventType.START_OF_SPEECH:
        if (this.#pendingEndOfSpeech) {
          return;
        }
        this.#hasTranscriptForCurrentSpeech = false;
        this.#put(event);
        return;

      case stt.SpeechEventType.FINAL_TRANSCRIPT:
        this.#hasTranscriptForCurrentSpeech = true;
        this.#put(event);
        this.#flushPendingEndOfSpeech();
        return;

      case stt.SpeechEventType.END_OF_SPEECH:
        if (this.#hasTranscriptForCurrentSpeech) {
          this.#put(event);
          this.#hasTranscriptForCurrentSpeech = false;
        } else {
          this.#pendingEndOfSpeech = event;
        }
        return;

      default:
        this.#put(event);
    }
  }

  #flushPendingEndOfSpeech(): void {
    if (!this.#pendingEndOfSpeech) {
      return;
    }

    this.#put(this.#pendingEndOfSpeech);
    this.#pendingEndOfSpeech = undefined;
    this.#hasTranscriptForCurrentSpeech = false;
  }

  #put(event: stt.SpeechEvent): void {
    if (!this.queue.closed) {
      this.queue.put(event);
    }
  }
}
