# Final M197 remote qualification harness

The source-frozen `remote-plan.json` declares the only online workload. The runner
compiles exact A and B broker/SDK snapshots and verifies their tracked file maps
before and after execution. No reset, shared-resource deletion or broad discovery
is performed.

The comparable plain profile uses eight ABBAABBA legs and the original wall
median ratio 1.1 and Receive p95 ratio 1.2. Escaped comparative timing is unavailable:
the original baseline loses NUL bytes in returned TEXT. The historical full-profile
comparison remains failed; this scoped qualification does not supersede it.

B alone must pass the unchanged escaped profile, both official clients and the
six-field Peek/Receive/live-attempt replay/Deferred/DLQ fixture. A plain and B mixed
180-second workloads are independent stability evidence; no throughput ratio is
computed between different work. All identity, metadata, delivery, settlement,
resource and cleanup accounting remains strict.

Provider observation on both variants preserves response bytes and retains only
sanitized approved-host HTTP error summaries. A passive ten-second observation
grace fits inside the original sixty-second cleanup budget. It does not establish
absence of a detached provider stream. Any observed HTTP error stops qualification.

The original fifteen-second caller, forty-five-second open, sixty-second lease,
240-second scenario plus sixty-second cleanup and 1800-second workload controller
budgets remain fixed. The1800-second controller includes archives, builds, local controls and
source cleanup, using SIGALRM1795 plus a five-second cleanup reserve. The
forty-minute workflow bounds setup, controller and artifact upload. Final physical integrity, restore and 48-hour product
acceptance remain separate release gates.
