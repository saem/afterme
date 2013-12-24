=======
afterme
=======
## Wat?

This is supposed to end up being a simple append only list datastore with a log file structure (append only). You can think of it as WAL (write ahead log) server.

## Why?

The thinking goes that this is a useful primitive in creating command (before taking action) and event (after action has been taken) logs that application developers can use to do things like provide total orders, CQRS, event sourcing, application level replication or maybe they just want a logging server with a few more guarntees.

## How?

When it's done it should accept http messages, with payload up to a certain configured limit, once the transfer is complete it and a smaller header (sequence number, data integrity hash ?, timestamp, timezone, content hash, content size) are appended to a file.

Crashes just mean you lose whatever was partially written, though you could try for manual recovery.

Streaming writes means afterme should be able to hit close to 200MB/s on 7500RPM spinning rust, SSDs will be faster. After hitting maybe 1GB file, start a new log, that way we can rotate and keep file sizes manageable.

## Unknowns

### Transactions/Guarantees
Commits, simply return a 200 OK as soon as it's in memory, once flushed to disk, inform via a websocket or web hook? I'm not sure about the exact details around this, I suspect this will be clear once I've fleshed it out and tried playing with it.

### To FSync or not to FSync
My current thinking is coalesce writes in memory, and flush when over a certain size or a few (10?) milliseconds have passed, all of this should happen in a separate goroutine.

### Querying and Reads
Except for perhaps a web socket that can be listened in on, or a plugin that fires events into Rabbit (or whatever) nothing first class will be supported for reads/queries. All querying support will either be through direct file access (the file format is versioned) and/or perhaps I might provide a utility that tails across the files and pipe them all together. Or maybe another daemon that handles reads...

## Licence

GPL 3 -- this could be negotiated.
