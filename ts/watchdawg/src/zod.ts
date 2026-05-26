// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

// zod is really picky about equality with different package versions.
// Preferably, install zod with peer dependencies and resolve the version to
// the same version as the zod in the rest of your project.  But this zod is
// exposed from here just in case.
export * from "zod";
