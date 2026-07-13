# Quarantine

Files in this folder are upstream SpiceDB conformance fixtures that Spiceport could not pass
faithfully at the time they were considered for vendoring. They are never edited to force a
pass and never silently dropped — each entry below records the filename, the unsupported
construct/assertion, and the actual failure observed. `QuarantinedCorpusTests` enumerates this
folder and reports each file as an explicitly skipped test so the gap stays visible in every
test run, not just in this document.

This folder is excluded from the main corpus loaders (`ConformanceTests`, `ValidationBlockTests`,
`ReverseConsistencyCrossCheckTests`), which only enumerate `TestData/*.yaml` non-recursively.

## Currently quarantined files

None. As of the v1.44.2 upstream diff, every file upstream vendors under
`internal/services/integrationtesting/testconfigs/` that Spiceport also carries is content-identical
(module drift limited to whitespace/quote-style cosmetics, reconciled in favor of upstream) or
intentionally retained in its Spiceport-specific form where the Spiceport version carries
additional assertions beyond upstream's (see `ConformanceTests`/repo history for `lrordering.yaml`
and `recursivearrowref.yaml`). Upstream introduced no files Spiceport lacks at this tag, so there
was nothing new to quarantine.
