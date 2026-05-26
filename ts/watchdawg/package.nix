# Copyright © 2026 Anterior <tech@anterior.com>
# SPDX-License-Identifier: AGPL-3.0-only

{ package-lock2nix, nodejs }:
package-lock2nix.mkNpmModule {
  name = "watchdawg";
  buildInputs = [ nodejs ];
  src = ./.;
}
