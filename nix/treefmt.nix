{ ... }:
{
  perSystem.treefmt = {
    projectRootFile = "flake.nix";
    programs = {
      # keep-sorted start block=true
      keep-sorted.enable = true;
      nixfmt = {
        enable = true;
        strict = true;
      };
      prettier.enable = true;
      # keep-sorted end
    };
  };
}
