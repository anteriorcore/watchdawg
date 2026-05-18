{ package-lock2nix, nodejs }:
package-lock2nix.mkNpmModule {
  name = "watchdawg";
  buildInputs = [ nodejs ];
  src = ./.;
}
