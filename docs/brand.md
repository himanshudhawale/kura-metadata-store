# Kura Metadata Store Brand Assets

## Problem

Kura Metadata Store needs a recognizable identity that belongs to the Kura
family while remaining distinct from the query engine. The mark must remain
clear in a README, repository preview, documentation, and small icon contexts
without licensed artwork or external font dependencies.

## Design

The shared isometric cube and geometric `K` establish the Kura family. Violet
distinguishes coordination metadata from Engine computation. Three nodes on
the lower cube vertices represent small authoritative records participating in
one consistent system.

## Assets

| Asset | Purpose |
|---|---|
| `assets/kura-metadata-store-icon.svg` | Square README, documentation, and avatar source |
| `assets/kura-metadata-store-social.svg` | 2:1 repository social-preview source |

SVG is the source of truth. Render social-preview raster files at `1280x640` or
larger without changing the aspect ratio.

## Usage

Keep clear space around the icon equal to at least one tenth of its width. Do
not remove the metadata nodes, rotate the cube, stretch the mark, add effects,
or recolor individual edges. The dark field is part of the square icon.

The social source uses selectable system-font text. Convert text to paths
during export when exact cross-platform typography is required.

## Alternatives

A database cylinder was rejected as too generic. A Raft cluster diagram was
rejected because the current implementation must not imply distributed
availability. A standalone `K` was rejected because it would not communicate
the relationship between Kura projects.

## Validation

The assets use only SVG primitives, include accessible titles and descriptions,
and fit square and 2:1 view boxes. Review at 32, 64, 256, and 1280 pixels before
publishing raster exports. Confirm readability in both light and dark GitHub
themes and ensure the mark does not imply capabilities beyond the README.
