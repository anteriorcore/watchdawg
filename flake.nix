# Copyright © 2026 Anterior <tech@anterior.com>
# SPDX-License-Identifier: AGPL-3.0-only

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

{
  inputs = {
    # keep-sorted start block=true
    flake-parts.url = "flake-parts";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
    package-lock2nix = {
      url = "github:anteriorai/package-lock2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-parts.follows = "flake-parts";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };
    tools = {
      url = "github:anteriorcore/tools";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.treefmt-nix.follows = "treefmt-nix";
      inputs.flake-parts.follows = "flake-parts";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # keep-sorted end
  };
  #  TODO: devshell, process-compose

  outputs =
    { self, flake-parts, ... }@inputs:
    let
      allSystems = {
        perSystem =
          {
            pkgs,
            inputs',
            system,
            lib,
            ...
          }:
          let
            nodejs = pkgs.nodejs_24;
            callPackage = lib.callPackageWith (
              pkgs
              // {
                inherit nodejs self;
                inherit (inputs.tools.nixosModules) elasticmq;
                package-lock2nix = pkgs.callPackage inputs.package-lock2nix.lib.package-lock2nix {
                  inherit nodejs;
                };
              }
            );
            watchdawg = callPackage ./ts/watchdawg/package.nix { };
            integration-test = callPackage ./nix/intergration-test.nix { };
          in
          {
            _module.args.pkgs = import inputs.nixpkgs {
              config.allowUnfree = true;
              inherit system;
              overlays = [
                (final: prev: {
                  inherit (inputs.tools.packages.${final.stdenv.hostPlatform.system}) wait-for-port;
                })
              ];
            };
            packages = {
              inherit (inputs'.tools.packages) conventional-commit nix-flake-check-changed nix-grep-to-build;
              inherit watchdawg;
              orchestrator = callPackage ./ts/fake-orchestrator/package.nix { };
            };
            checks =
              { }
              # Unfortuntely seems like it only succeeds on x86_64-linux on
              # GitHub.  Needs linux so darwin doens't work and it should work
              # on aarch64-darwin but it doesn't seem to finish.
              // (lib.mkIf (system == "x86_64-linux") { inherit integration-test; });
            legacyPackages = { inherit integration-test; };
          };
      };
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-linux"
      ];
      imports = [
        # keep-sorted start
        ./nix/treefmt.nix
        allSystems
        inputs.tools.flakeModules.checkBuildAll
        inputs.treefmt-nix.flakeModule
        # keep-sorted end
      ];
    };
}
