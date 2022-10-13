{
  outputs = { nixpkgs, ... }:
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
    stdenv = pkgs.llvmPackages_12.stdenv;
    mkShell = pkgs.mkShell.override { inherit stdenv; };
  in
  {
    devShell.x86_64-linux = mkShell {
      packages = with pkgs; [
        cmakeCurses
        clang-tools_12
      ];
      hardeningDisable = [ "fortify" ];
    };
  };
}
