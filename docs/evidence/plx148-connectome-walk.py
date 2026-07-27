#!/usr/bin/env python3
"""PLX-148 — walk the served Connectome and fetch every Dynamic child.

A client, not a fixture. It is handed a host and a port and nothing else: it
fetches `substrate.connectome {}`, and every path it asks for afterwards it
read off the document that came back. If the document does not say a child
exists, this program never learns of it.

Every frame is logged with its byte count, so the round-trip claim is counted
rather than described.
"""
import asyncio, json, sys, hashlib
import websockets

URL = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:4477"
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/plx148"

FETCHES = 0
BYTES = 0

async def fetch(ws, params, label):
    """One `substrate.connectome` subscription, returning the document."""
    global FETCHES, BYTES
    req = {"jsonrpc": "2.0", "id": FETCHES + 1,
           "method": "substrate.connectome", "params": params}
    raw = json.dumps(req)
    await ws.send(raw)
    FETCHES += 1
    print(f"  -> FETCH {label:<32} substrate.connectome {json.dumps(params)}"
          f"  ({len(raw)} B out)")
    doc, err = None, None
    while True:
        msg = await asyncio.wait_for(ws.recv(), timeout=30)
        BYTES += len(msg)
        m = json.loads(msg)
        if "result" in m:                      # subscription id
            continue
        if "params" in m and "subscription" in m.get("params", {}):
            item = m["params"]["result"]
            kind = item.get("type")
            print(f"  <- FRAME {label:<32} type={kind:<6} {len(msg)} B")
            if kind == "error":
                err = item.get("message")
                break
            if kind == "data":
                doc = item.get("content")
                continue
            if kind == "done":
                break
    return doc, err


def edges(node, path, out):
    """Every child edge, as (kind, path, advertised-hash), read off the doc.

    The paths this builds are the only thing the fetches below are allowed to
    use — nothing is composed from knowledge of substrate.
    """
    for c in node.get("children", []):
        p = f"{path}/{c['namespace']}" if path else c["namespace"]
        kind = c["edge"]
        if kind == "dynamic":
            out.append(("dynamic", p, c["hash"]))
        elif kind == "indexed":
            out.append(("indexed", p, c["template"].get("hash", "")))
            edges(c["template"], p, out)
        else:
            out.append(("static", p, c.get("hash", "")))
            edges(c, p, out)


async def main():
    async with websockets.connect(URL, max_size=None) as ws:
        print("PLX-148 — one document fetch, then every Dynamic child")
        print("=" * 68)
        root, err = await fetch(ws, {}, "root")
        assert root and not err, f"the root document did not arrive: {err}"
        raw = json.dumps(root)
        print(f"\nROOT  namespace={root['namespace']}  ir_hash={root.get('ir_hash')}")
        print(f"      hash_algorithm={root.get('hash_algorithm')}"
              f"  backend_name={root.get('backend_name')}"
              f"  respond_method={root.get('respond_method')}")
        print(f"      methods={len(root.get('methods', []))}"
              f"  children={len(root.get('children', []))}"
              f"  bytes={len(raw)}")
        open(f"{OUT}-root.json", "w").write(json.dumps(root, indent=2))

        found = []
        edges(root, "", found)
        dyn = [(p, h) for (k, p, h) in found if k == "dynamic"]
        static = [e for e in found if e[0] == "static"]
        indexed = [e for e in found if e[0] == "indexed"]
        print(f"\nWALKED LOCALLY: {len(static)} Static + {len(indexed)} Indexed"
              f" edges, 0 fetches")
        print(f"DYNAMIC EDGES ADVERTISED: {len(dyn)}\n")

        ok = fail = 0
        for path, advertised in dyn:
            doc, err = await fetch(ws, {"namespace": path}, path)
            if doc is None:
                print(f"     UNFETCHABLE {path}: {err}")
                fail += 1
                continue
            match = doc.get("hash") == advertised
            print(f"     ns={doc['namespace']:<12} methods={len(doc.get('methods', [])):<3}"
                  f" hash={doc.get('hash')[:16]}…  advertised-matches={match}")
            open(f"{OUT}-{path.replace('/', '_')}.json", "w").write(json.dumps(doc, indent=2))
            ok += 1 if match else 0
            fail += 0 if match else 1

        print("\n" + "=" * 68)
        print(f"DYNAMIC EDGES: {len(dyn)}   FETCHED+HASH-VERIFIED: {ok}   FAILED: {fail}")
        print(f"TOTAL FETCHES: {FETCHES}  (1 document + {FETCHES - 1} lazy)")
        print(f"TOTAL BYTES IN: {BYTES}")
        return 0 if fail == 0 else 1

sys.exit(asyncio.run(main()))
