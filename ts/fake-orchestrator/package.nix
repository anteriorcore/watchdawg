{ package-lock2nix, nodejs }:
package-lock2nix.mkNpmModule {
  name = "orchestrator";
  buildInputs = [ nodejs ];
  src = ./.;
}
