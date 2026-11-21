# Data Format and Encryption

go-workflow records a run's data as the [Workflow SDK](https://workflow-sdk.dev)
does, so a Go run reads as SDK data: the Workflow web UI, Vercel's dashboard,
and other SDK tools decode its inputs, outputs, errors, hook payloads, and
streams with their own code. Any World can also encrypt that data.

## Payloads

Every payload starts with a 4-byte label, and labels nest:

| Label | Content |
| --- | --- |
| `devl` | The value, in [devalue](https://github.com/sveltejs/devalue) text. |
| `gzip`, `zstd` | A compressed payload. |
| `encr` | A payload encrypted with the run's key (AES-256-GCM). |
| `encp` | A payload sealed to the run's public key (X25519 and AES-256-GCM). |

Values are encoded with the SDK's types, field names following `json` tags:

| Go | SDK value |
| --- | --- |
| structs, `map[string]T` | objects |
| slices, arrays | arrays |
| strings, numbers, bools, `nil` | the same |
| `[]byte` | `Uint8Array` |
| `time.Time` | `Date`, to the millisecond |
| `*big.Int` | `BigInt` |
| maps with other keys | `Map` |
| `*url.URL` | `URL` |
| `http.Header` | `Headers` |
| `error` | `Error`, with `FatalError` and `RetryableError` |

A type with a `MarshalJSON` method is encoded from its JSON. To choose its own
encoding, a type implements `worlds.PayloadMarshaler` and
`worlds.PayloadUnmarshaler`.

A run's input is its argument list, `[input]`, and a step's input is
`{args: [input]}`, as the SDK records them. Payloads of 1 KiB or more are
compressed when that saves at least 5%: with zstd at level 3, as the SDK does,
or with gzip when `WORKFLOW_COMPRESSION_CODEC=gzip`. `WORKFLOW_DISABLE_COMPRESSION=1` turns
compression off.

A stream is a stream of values, as the SDK's `getWritable()` writes one: each
`Write` stores one frame, a big-endian u32 length and the chunk's payload, a
`Uint8Array`. Readers return the bytes written.

## Encryption

A World encrypts a run's payloads with a key it resolves for the run, through
the optional `worlds.EncryptionKeys` interface, the SDK's
`getEncryptionKeyForRun`. The runtime and the client encrypt and decrypt;
Worlds store payloads as opaque bytes and never open them.

The local, Postgres, AWS, and test Worlds take a 32-byte key:

```go
key, err := base64.StdEncoding.DecodeString(os.Getenv("WORKFLOW_ENCRYPTION_KEY")) // openssl rand -base64 32
if err != nil {
	log.Fatal(err)
}
world, err := postgresworld.Open(ctx, postgresworld.Config{
	BaseURL:       os.Getenv("WORKFLOW_BASE_URL"),
	URL:           os.Getenv("DATABASE_URL"),
	EncryptionKey: key,
})
```

Each run's key is derived from it with HKDF-SHA256, as the SDK derives a run's
key from a deployment key. Changing the key leaves earlier runs' payloads
unreadable. The test World takes one with `SetEncryptionKey`.

The [Vercel World](/worlds/vercel#encryption) resolves keys as the SDK does on
Vercel, from the deployment's key, so it needs no configuration.

### What is encrypted

A run's own writes, such as its input, step inputs and results, hook metadata,
and its output or error, are encrypted with the run's key (`encr`). A run with
a key publishes its public key, `encryptionPublicKey` on the run and in each
hook's resume context, and what is sent to it from outside is sealed to that
key (`encp`): hook and webhook payloads, and chunks written with `Run.Writer`.
A sender that seals can never read the run back. Names, IDs, times,
attributes, and statuses stay plaintext.

### Reading encrypted runs

`workflow.NewClient` reads a run's key from its World. `workflow.NewHTTPClient`
and the [world-http adapter](/tooling/world-http) read it over the
`runs.encryptionKey` RPC method, behind the
[client endpoints' protection](/worlds/#protecting-the-client-endpoints), so
the [Workflow web UI](/tooling/web-ui) decrypts Go runs as it does JS runs.
