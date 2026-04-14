#!/usr/bin/env bash
set -euo pipefail

APP_NAME="pgdumpcloud"
BINARY_NAME="pgdumpcloud"
BUNDLE_DIR="src-tauri/target"
INSTALL_DIR="/usr/local/bin"
OS="$(uname -s)"

print_usage() {
    echo "Usage: ./build.sh <command> [target]"
    echo ""
    echo "Commands:"
    echo "  cli        Build the CLI and install to ${INSTALL_DIR}"
    echo "  app        Build the desktop app for a given target"
    echo ""
    echo "App targets (macOS):"
    echo "  arm        Build for Apple Silicon (M1/M2/M3/M4)"
    echo "  intel      Build for Intel Macs"
    echo "  universal  Build universal binary (both architectures)"
    echo "  mac-all    Build all macOS variants"
    echo ""
    echo "App targets (Linux):"
    echo "  linux      Build for the current architecture"
    echo "  linux-x64  Build for x86_64"
    echo "  linux-arm  Build for aarch64 (ARM64)"
    echo ""
    echo "App meta targets:"
    echo "  all        Build all targets for the current platform"
    echo ""
    echo "Examples:"
    echo "  ./build.sh cli"
    echo "  ./build.sh app arm"
    echo "  ./build.sh app universal"
    echo "  ./build.sh app all"
}

# ── CLI build ──

build_cli() {
    echo "━━━ Building ${BINARY_NAME} CLI in release mode ━━━"
    cargo build --release -p pgdumpcloud-core

    local binary_path="target/release/${BINARY_NAME}"
    if [ ! -f "$binary_path" ]; then
        echo "Error: binary not found at ${binary_path}"
        exit 1
    fi

    echo "Installing ${BINARY_NAME} to ${INSTALL_DIR}..."
    sudo install -m 755 "$binary_path" "${INSTALL_DIR}/${BINARY_NAME}"
    echo "✓ Installed: $(which $BINARY_NAME) → $($BINARY_NAME --version 2>/dev/null || echo "${BINARY_NAME}")"
}

# ── Rust target helpers ──

ensure_mac_targets() {
    if ! rustup target list --installed | grep -q "aarch64-apple-darwin"; then
        echo "Installing aarch64-apple-darwin target..."
        rustup target add aarch64-apple-darwin
    fi
    if ! rustup target list --installed | grep -q "x86_64-apple-darwin"; then
        echo "Installing x86_64-apple-darwin target..."
        rustup target add x86_64-apple-darwin
    fi
}

ensure_linux_target() {
    local target="$1"
    if ! rustup target list --installed | grep -q "$target"; then
        echo "Installing ${target} target..."
        rustup target add "$target"
    fi
}

get_app_version() {
    grep '"version"' src-tauri/tauri.conf.json | head -1 | sed 's/.*: "//;s/".*//'
}

# ── macOS app builds ──

build_mac_arm() {
    local target_dir="${BUNDLE_DIR}/aarch64-apple-darwin/release/bundle"
    echo "━━━ Building app for macOS Apple Silicon (arm64) ━━━"
    bun run tauri build --target aarch64-apple-darwin
    echo "✓ .app: ${target_dir}/macos/${APP_NAME}.app"
    echo "✓ .dmg: ${target_dir}/dmg/${APP_NAME}_$(get_app_version)_aarch64.dmg"
}

build_mac_intel() {
    local target_dir="${BUNDLE_DIR}/x86_64-apple-darwin/release/bundle"
    echo "━━━ Building app for macOS Intel (x86_64) ━━━"
    bun run tauri build --target x86_64-apple-darwin
    echo "✓ .app: ${target_dir}/macos/${APP_NAME}.app"
    echo "✓ .dmg: ${target_dir}/dmg/${APP_NAME}_$(get_app_version)_x64.dmg"
}

build_mac_universal() {
    local target_dir="${BUNDLE_DIR}/universal-apple-darwin/release/bundle"
    echo "━━━ Building macOS Universal binary (arm64 + x86_64) ━━━"
    bun run tauri build --target universal-apple-darwin
    echo "✓ .app: ${target_dir}/macos/${APP_NAME}.app"
    echo "✓ .dmg: ${target_dir}/dmg/${APP_NAME}_$(get_app_version)_universal.dmg"
}

# ── Linux app builds ──

build_linux_x64() {
    ensure_linux_target "x86_64-unknown-linux-gnu"
    echo "━━━ Building app for Linux x86_64 ━━━"
    bun run tauri build --target x86_64-unknown-linux-gnu
    echo "✓ Linux x64 build: ${BUNDLE_DIR}/x86_64-unknown-linux-gnu/release/bundle/"
}

build_linux_arm() {
    ensure_linux_target "aarch64-unknown-linux-gnu"
    echo "━━━ Building app for Linux aarch64 ━━━"
    bun run tauri build --target aarch64-unknown-linux-gnu
    echo "✓ Linux ARM build: ${BUNDLE_DIR}/aarch64-unknown-linux-gnu/release/bundle/"
}

build_linux_native() {
    echo "━━━ Building app for Linux (native architecture) ━━━"
    bun run tauri build
    echo "✓ Linux native build: ${BUNDLE_DIR}/release/bundle/"
}

# ── Main ──

if [ $# -eq 0 ]; then
    print_usage
    exit 1
fi

case "$1" in
    cli)
        build_cli
        ;;
    app)
        if [ $# -lt 2 ]; then
            echo "Error: 'app' requires a target argument"
            echo ""
            print_usage
            exit 1
        fi
        case "$2" in
            arm)
                ensure_mac_targets
                build_mac_arm
                ;;
            intel)
                ensure_mac_targets
                build_mac_intel
                ;;
            universal)
                ensure_mac_targets
                build_mac_universal
                ;;
            mac-all)
                ensure_mac_targets
                build_mac_arm
                build_mac_intel
                build_mac_universal
                ;;
            linux)
                build_linux_native
                ;;
            linux-x64)
                build_linux_x64
                ;;
            linux-arm)
                build_linux_arm
                ;;
            all)
                if [ "$OS" = "Darwin" ]; then
                    ensure_mac_targets
                    build_mac_arm
                    build_mac_intel
                    build_mac_universal
                elif [ "$OS" = "Linux" ]; then
                    build_linux_native
                fi
                ;;
            *)
                echo "Unknown app target: $2"
                print_usage
                exit 1
                ;;
        esac
        ;;
    *)
        echo "Unknown command: $1"
        print_usage
        exit 1
        ;;
esac

echo ""
echo "Done."
