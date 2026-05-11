{
  description = "fc-rs: Fast block-order copy with dedup (Rust native)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    fenix.url = "github:nix-community/fenix";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      crane,
      fenix,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        toolchain = fenix.packages.${system}.stable.toolchain;
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;

        commonArgs = {
          src = craneLib.cleanCargoSource ./.;
          nativeBuildInputs = with pkgs; [ pkg-config ];
          buildInputs = with pkgs; [
            openssl
            libssh2
            zlib
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        fc-rs-unwrapped = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
          }
        );

        fc-rs =
          pkgs.runCommand "fc-rs"
            {
              nativeBuildInputs = [ pkgs.makeWrapper ];
              buildInputs = with pkgs; [
                openssl
                libssh2
                zlib
              ];
              inherit fc-rs-unwrapped;
            }
            ''
              mkdir -p $out/bin
              makeWrapper ${fc-rs-unwrapped}/bin/fc-rs $out/bin/fc-rs \
                --prefix LD_LIBRARY_PATH : ${
                  pkgs.lib.makeLibraryPath (
                    with pkgs;
                    [
                      openssl
                      libssh2
                      zlib
                    ]
                  )
                }
            '';

        # Python reference implementation
        fast-copy-python = pkgs.callPackage ./nix/fast-copy-python.nix {
          fast-copy-src = ./vendor/fast-copy;
        };

        # Test data generator
        testData = pkgs.runCommand "fc-rs-test-data" { } ''
          mkdir -p $out

          echo "hello world" > $out/hello.txt
          echo "small file content here" > $out/small.txt
          printf "line1\nline2\nline3\n" > $out/multiline.txt

          mkdir -p $out/sub/deep
          echo "deep nested" > $out/sub/deep/nested.txt
          echo "sub file" > $out/sub/subfile.txt
          mkdir -p $out/sub/empty_dir

          dd if=/dev/urandom bs=1024 count=2 of=$out/random-2k.bin 2>/dev/null
          dd if=/dev/urandom bs=1024 count=10 of=$out/random-10k.bin 2>/dev/null

          touch $out/empty.txt

          echo "dedup-me" > $out/dedup_original.txt
          cp $out/dedup_original.txt $out/dedup_copy1.txt
          cp $out/dedup_original.txt $out/dedup_copy2.txt

          dd if=/dev/urandom bs=1M count=2 of=$out/large-2m.bin 2>/dev/null
          dd if=/dev/zero bs=1M count=1 of=$out/zero-1m.bin 2>/dev/null

          echo "spaces" > "$out/file with spaces.txt"
          echo "parens" > "$out/file_(1).txt"
        '';

        # Build a pkgs set with our packages overlaid for the test
        pkgsForTest = import nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              inherit fc-rs fast-copy-python testData;
            })
          ];
        };

      in
      {
        packages = {
          default = fc-rs;
          fast-copy-python = fast-copy-python;
          testData = testData;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = with pkgs; [
            toolchain
            cargo-edit
            cargo-watch
            rust-analyzer
          ];
          shellHook = ''
            echo "Rust dev environment (crane)"
            echo "Commands: cargo build, cargo test, cargo fmt"
          '';
        };

        checks = {
          formatting =
            pkgs.runCommand "check-formatting"
              {
                nativeBuildInputs = with pkgs; [
                  nixfmt
                  cargo
                  rustfmt
                ];
                src = ./.;
              }
              ''
                cd $src
                nixfmt --check *.nix
                cargo fmt --check
                touch $out
              '';

          tests = self.packages.${system}.default;

          integration = pkgsForTest.testers.nixosTest (
            { pkgs, lib, ... }:
            import ./nixos/tests/parity.nix {
              pkgs = pkgsForTest;
              fc-rs = pkgsForTest.fc-rs;
              fast-copy-python = pkgsForTest.fast-copy-python;
              testData = pkgsForTest.testData;
            }
          );
        };

        formatter = pkgs.nixfmt;
      }
    );
}
