{
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.rust-overlay = {
    url = "github:oxalica/rust-overlay";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  inputs.crane.url = "github:ipetkov/crane";
  inputs.nix2container = {
    url = "github:nlewo/nix2container";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
    crane,
    nix2container,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [(import rust-overlay)];
          config = {
            allowUnfree = true;
          };
        };
        # Same toolchain as rustup users
        rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        # Same channel, without clippy/rust-analyzer/rust-src: the build only needs rustc + cargo
        channel = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml)).toolchain.channel;
        craneLib = (crane.mkLib pkgs).overrideToolchain (p:
          p.rust-bin.fromRustupToolchain {
            inherit channel;
            profile = "minimal";
          });
        commonArgs = {
          # Cargo.toml, Cargo.lock and *.rs only, so README or hook edits don't rebuild
          src = craneLib.cleanCargoSource ./.;
          strictDeps = true;
          # fmt, clippy and tests run locally in the pre-commit hook (.githooks/pre-commit)
          doCheck = false;
          # One rustc at a time: matrix-sdk is memory hungry and the build pod has 3Gi
          CARGO_BUILD_JOBS = "1";
        };
        # Dependencies in their own derivation, so source changes don't rebuild matrix-sdk (once cached)
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        kubbot = craneLib.buildPackage (commonArgs // {inherit cargoArtifacts;});
      in {
        packages =
          {
            default = kubbot;
          }
          // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
            image = nix2container.packages.${system}.nix2container.buildImage {
              name = "kubbot";
              # Separate layers per store path (glibc, kubbot, …)
              maxLayers = 100;
              config = {
                Entrypoint = ["${kubbot}/bin/kubbot"];
                Env = [
                  "DATA_DIR=/data"
                  # rustls-platform-verifier reads the system roots
                  "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                ];
                User = "65532:65532";
                # Commit date (git+file flake source); infra's release-image never moves a tag back to an older one
                Labels."ci.casasulgorgo.net/commit-timestamp" = toString self.lastModified;
              };
            };
          };

        devShells.default = pkgs.mkShell {
          __darwinAllowLocalNetworking = true;
          shellHook = ''
            echo "Hello!"
            git config core.hooksPath .githooks
          '';
          packages = with pkgs; [
            rust
          ];
        };
      }
    );
}
