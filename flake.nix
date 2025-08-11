{
  description = "Rust development environment with fenix, crane, and shell inheritance";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Rust toolchain configuration
        # Prüft ob rust-toolchain.toml existiert, sonst stable channel
        toolchain =
          if builtins.pathExists ./rust-toolchain.toml then
            fenix.packages.${system}.fromToolchainFile {
              file = ./rust-toolchain.toml;
              sha256 = pkgs.lib.fakeSha256;
            }
          else
            fenix.packages.${system}.stable.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
            ];

        # Rust-analyzer nightly für beste LSP features
        rustAnalyzer = fenix.packages.${system}.rust-analyzer;

        # Crane für optimiertes Building
        craneLib = crane.mkLib pkgs;
        craneLibWithToolchain = craneLib.overrideToolchain toolchain;

        # Common build inputs für alle Targets
        commonBuildInputs =
          with pkgs;
          [
            openssl
            pkg-config
            libiconv
            sqlite
          ]
          ++ lib.optionals stdenv.isLinux [
            # Linux-spezifische Dependencies
          ];

        # Cargo artifacts für Caching
        cargoArtifacts = craneLibWithToolchain.buildDepsOnly {
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          buildInputs = commonBuildInputs;
          # Wichtig für cross-compilation und native deps
          strictDeps = true;
        };

        # Das eigentliche Rust Package
        rustPackage = craneLibWithToolchain.buildPackage {
          inherit cargoArtifacts;
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          buildInputs = commonBuildInputs;
          strictDeps = true;
        };

        # Shell Tools aus deiner home-manager config
        shellTools = with pkgs; [
          # Terminal & Shell (aus deiner shell.nix)
          nushell
          starship
          atuin
          carapace

          # File Management
          eza
          fd
          ripgrep
          bat
          yazi
          zoxide

          # Development Tools
          helix
          lazygit
          direnv
          tealdeer
          pueue

          # System Info
          fastfetch
          btop

          # Secrets Management
          sops
          age # Falls du age keys verwendest
        ];

        # Rust-spezifische Dev Tools
        rustDevTools = with pkgs; [
          # Build Acceleration
          sccache # Compiler cache für 5-10x schnellere rebuilds

          # Cargo plugins für besseren Workflow
          cargo-watch # Auto-rebuild bei Änderungen
          cargo-nextest # Schnellerer Test Runner
          cargo-audit # Security Auditing
          cargo-outdated # Dependency Updates
          cargo-edit # cargo add/rm/upgrade
          cargo-expand # Macro expansion
          cargo-flamegraph # Performance profiling
          bacon # Background rust compiler

          # Code Quality
          cargo-tarpaulin # Code Coverage
          cargo-deny # Supply chain security
          cargo-machete # Unused dependency detection

          # Optional: WebAssembly Support
          # wasm-pack
          # wasm-bindgen-cli
        ];

      in
      {
        # Packages
        packages = {
          default = rustPackage;

          # Weitere Cargo commands als packages
          clippy = craneLibWithToolchain.cargoClippy {
            inherit cargoArtifacts;
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            buildInputs = commonBuildInputs;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          };

          test = craneLibWithToolchain.cargoTest {
            inherit cargoArtifacts;
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            buildInputs = commonBuildInputs;
          };

          doc = craneLibWithToolchain.cargoDoc {
            inherit cargoArtifacts;
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            buildInputs = commonBuildInputs;
          };

          fmt-check = craneLibWithToolchain.cargoFmt {
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            cargoFmtExtraArgs = "-- --check";
          };
        };

        # Development Shells
        devShells = {
          default = pkgs.mkShell {
            buildInputs = [
              toolchain
              rustAnalyzer
            ]
            ++ commonBuildInputs
            ++ shellTools
            ++ rustDevTools;

            # Environment Variables
            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
            RUST_BACKTRACE = "1";
            RUST_LOG = "debug";

            # Für bessere Performance in dev
            CARGO_BUILD_JOBS = "8";
            CARGO_TARGET_DIR = "target";

            # Editor integration
            EDITOR = "hx";

            # Shell Hook mit Home-Manager Integration
            shellHook = ''
              # Home-Manager Session Variables laden
              if [ -f ~/.nix-profile/etc/profile.d/hm-session-vars.sh ]; then
                source ~/.nix-profile/etc/profile.d/hm-session-vars.sh
              fi

              # Nushell config directory für custom scripts
              export NU_LIB_DIRS="$PWD/.nu-scripts:$HOME/.config/nushell/scripts"

              # Projekt Root für Scripts
              export PROJECT_ROOT="$PWD"

              # Rust Projekt Info
              echo "🦀 Rust Development Environment"
              echo "📦 Toolchain: $(rustc --version)"
              echo "🔧 Cargo: $(cargo --version)"
              echo "📝 rust-analyzer: $(rust-analyzer --version)"
              echo ""
              echo "Available commands:"
              echo "  cargo watch -x run    # Auto-rebuild on changes"
              echo "  cargo nextest run     # Fast test runner"
              echo "  bacon                 # Background compiler"
              echo "  nix build .#clippy    # Run clippy checks"
              echo "  nix build .#test      # Run tests"
              echo ""

              # Optional: Automatisch in Nushell wechseln wenn nicht bereits drin
              if [ -z "$NU_VERSION" ] && command -v nu >/dev/null 2>&1; then
                echo "Starting Nushell..."
                exec nu
              fi
            '';

            # Inherit von deiner direnv config
            inherit (import ./shell-inheritance.nix { inherit pkgs; })
              CARAPACE_BRIDGES
              ;
          };

          # Minimal shell ohne extra tools (für CI/CD)
          minimal = pkgs.mkShell {
            buildInputs = [
              toolchain
              rustAnalyzer
            ]
            ++ commonBuildInputs;

            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
          };

          # Nightly shell für experimental features
          nightly = pkgs.mkShell {
            buildInputs = [
              fenix.packages.${system}.latest.withComponents
              [
                "cargo"
                "clippy"
                "rust-src"
                "rustc"
                "rustfmt"
              ]
              fenix.packages.${system}.rust-analyzer
            ]
            ++ commonBuildInputs
            ++ shellTools
            ++ rustDevTools;

            RUST_SRC_PATH = "${fenix.packages.${system}.latest.rust-src}/lib/rustlib/src/rust/library";

            shellHook = ''
              echo "🌙 Rust Nightly Environment"
              echo "📦 $(rustc --version)"
            '';
          };

          # WASM Development Shell
          wasm = pkgs.mkShell {
            buildInputs = [
              (fenix.packages.${system}.stable.withComponents [
                "cargo"
                "clippy"
                "rust-src"
                "rustc"
                "rustfmt"
              ])
              rustAnalyzer
              pkgs.wasm-pack
              pkgs.wasm-bindgen-cli
              pkgs.binaryen # wasm-opt
            ]
            ++ commonBuildInputs;

            shellHook = ''
              echo "🕸️ Rust WASM Development Environment"
              rustup target add wasm32-unknown-unknown 2>/dev/null || true
            '';
          };
        };

        # Apps für direkte Ausführung
        apps = {
          default = flake-utils.lib.mkApp {
            drv = rustPackage;
          };

          # Entwicklungs-Commands als Apps
          watch = {
            type = "app";
            program = toString (
              pkgs.writeShellScript "cargo-watch" ''
                ${pkgs.cargo-watch}/bin/cargo-watch -x run
              ''
            );
          };

          test-watch = {
            type = "app";
            program = toString (
              pkgs.writeShellScript "test-watch" ''
                ${pkgs.cargo-watch}/bin/cargo-watch -x "nextest run"
              ''
            );
          };
        };

        # Checks für CI/CD
        checks = {
          inherit rustPackage;

          clippy = craneLibWithToolchain.cargoClippy {
            inherit cargoArtifacts;
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            buildInputs = commonBuildInputs;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          };

          fmt = craneLibWithToolchain.cargoFmt {
            src = craneLib.cleanCargoSource (craneLib.path ./.);
          };

          audit = craneLibWithToolchain.cargoAudit {
            inherit cargoArtifacts;
            src = craneLib.cleanCargoSource (craneLib.path ./.);
            advisory-db = pkgs.advisory-db;
          };
        };
      }
    );
}
