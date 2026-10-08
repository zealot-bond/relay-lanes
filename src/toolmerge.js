// Tool-call delta merging. Shared by the lane worker and the unit tests.
//
// This lives in its own module on purpose: the tests previously carried their own
// byte-identical copy of this algorithm, so a regression in the real one could not
// fail the suite -- the suite was green while a tool-argument aliasing bug was live
// in the worker. Tests must import the code they claim to test.

/**
 * Collapse streamed tool-call fragments into one entry per index, ACROSS batches.
 *
 * Batching is what makes streaming affordable, but naively concatenating the
 * fragments produced this on the wire:
 *
 *   delta.tool_calls = [ {index:0, function:{name:null, arguments:"{\"file_path\": "}},
 *                         {index:0, function:{name:null, arguments:"\""}}, ... ]
 *
 * Every fragment was its own array element, all sharing index 0, with no `id` and
 * a null `name`. A strict tool-use validator reads that as one call missing its
 * required properties -- which is exactly the reported failure:
 *   invalid arguments: missing required property "file_path" / "old_string" / "new_string"
 *
 * One entry per index, arguments concatenated, identity fields taken from the first
 * fragment that carries them, is what a client expects to reassemble.
 *
 * The accumulator is the caller's and outlives one flush, because identity and
 * arguments arrive in DIFFERENT 40ms windows: upstream sends id+name with the
 * first fragment and arguments over many. Merging a single batch in isolation
 * emitted an arguments-only entry with no id and no name -- the same malformed
 * call this function exists to prevent, just moved to a later boundary.
 */
/**
 * Append a streamed name fragment without duplicating it.
 *
 * Upstreams are inconsistent about the function name: most send it complete once,
 * some repeat it on every fragment, a few split it like the arguments. Blind
 * concatenation turns "edit" + "edit" into "editedit", which a harness reports as
 * an unknown tool; taking only the first fragment loses "it" when the name is
 * split. This handles all three shapes: a whole-name repeat and a tail repeat are
 * both recognised and skipped.
 */
function appendName (cur, piece) {
  if (!piece) return cur
  if (!cur) return piece
  if (piece === cur) return cur
  if (cur.endsWith(piece)) return cur
  return cur + piece
}

function mergeToolDeltas (frags, acc = new Map()) {
  const emitted = new Map()
  for (const d of frags) {
    const i = typeof d.index === 'number' ? d.index : 0
    const cur = acc.get(i) || {
      index: i, type: 'function', id: '',
      function: { name: '', arguments: '' },
      sentId: false, sentName: false,
    }
    const out = emitted.get(i) || { index: i, type: 'function', function: { arguments: '' } }

    if (d.id && !cur.id) cur.id = d.id
    if (d.type && !cur.type) cur.type = d.type

    // Identity fields are emitted once per call, on the batch where they first
    // appear. A client that assigns keeps the value it was given; one that appends
    // sees a single fragment and so also ends up with the exact name. Re-sending
    // them on every batch is what produced "editedit".
    if (cur.id && !cur.sentId) { out.id = cur.id; cur.sentId = true }
    if (d.function?.name && !cur.sentName) {
      cur.function.name = appendName(cur.function.name, d.function.name)
      out.function.name = cur.function.name
      cur.sentName = true
    } else if (d.function?.name) {
      // Later pieces still accumulate, so the final folded name is complete even
      // though the client only ever received the first fragment.
      cur.function.name = appendName(cur.function.name, d.function.name)
      if (out.function && typeof out.function.name === 'string') {
        out.function.name = cur.function.name
      }
    }
    // Arguments are the one field that genuinely streams in pieces: this batch's
    // own text is forwarded, while the accumulator keeps the full string.
    if (d.function?.arguments) {
      cur.function.arguments += d.function.arguments
      out.function.arguments += d.function.arguments
    }
    acc.set(i, cur)
    emitted.set(i, out)
  }
  return [...emitted.values()].sort((a, b) => a.index - b.index)
}

export { appendName, mergeToolDeltas }
