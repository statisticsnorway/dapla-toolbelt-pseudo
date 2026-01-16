{
  description = "Provide development environment for dapla-toolbelt";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = {nixpkgs, ...}: let
    systems = [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ];
    forAllSystems = function:
      nixpkgs.lib.genAttrs systems (system: function nixpkgs.legacyPackages.${system});
  in {
    devShells = forAllSystems (pkgs: {
      default = pkgs.mkShell {
        name = "dapla-toolbelt devel";
        packages = with pkgs; [
          pre-commit
          pipx
          python313Packages.ruff
          uv
          xz
          zlib
        ];
      };
    });
    formatter = forAllSystems (pkgs: pkgs.alejandra);
  };
}
