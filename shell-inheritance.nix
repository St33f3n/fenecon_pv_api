# shell-inheritance.nix
# Dieses Modul importiert Einstellungen aus deiner Home-Manager Konfiguration
{ pkgs }:

{
  # Environment Variables aus deiner shell.nix
  EDITOR = "hx";
  SHELL = "nu";
  CARAPACE_BRIDGES = "zsh,fish,bash,inshellisense";

  # SOPS Configuration
  SOPS_AGE_KEY_FILE = "$HOME/.config/sops/age/keys.txt";

  # Nushell spezifisch
  NU_LIB_DIRS = "$PWD/.nu-scripts:$HOME/.config/nushell/scripts";

  # Pager Setup
  PAGER = "less -R";
  MANPAGER = "sh -c 'col -bx | bat -l man -p'";

  # Development defaults
  RUST_BACKTRACE = "1";
  RUST_LOG = "debug";

  # Für bessere Shell-Integration
  shellAliases = {
    # System Utils (aus deiner config)
    c = "clear";
    ff = "fastfetch";
    wifi = "nmtui";

    # Modern tool replacements
    ls = "eza --icons --git --group-directories-first";
    ll = "eza --icons --git --group-directories-first -l";
    la = "eza --icons --git --group-directories-first -la";
    tree = "eza --icons --git --tree";
    lf = "yazi";

    # Git shortcuts
    gst = "git status";
    gco = "git checkout";
    gcm = "git commit -m";
    gp = "git push";
    gpl = "git pull";

    # Rust/Cargo shortcuts
    cb = "cargo build";
    cr = "cargo run";
    ct = "cargo test";
    cc = "cargo check";
    cw = "cargo watch -x run";
    cwt = "cargo watch -x 'nextest run'";
    cclippy = "cargo clippy -- -W clippy::all";
    cfmt = "cargo fmt";

    # Nix shortcuts für das Projekt
    nb = "nix build";
    nd = "nix develop";
    nf = "nix flake check";
    nfu = "nix flake update";
  };
}
