{ pkgs, fc-rs, fast-copy-python, testData }:
let
  inherit (pkgs) lib;
in {
  name = "fc-rs-parity-test";

  nodes.machine = { config, pkgs, lib, ... }: {
    virtualisation.writableStore = true;
    virtualisation.memorySize = 2048;
    virtualisation.diskSize = 4096;

    virtualisation.emptyDiskImages = [ 2048 ];

    environment.systemPackages = with pkgs; [
      fc-rs
      fast-copy-python
      coreutils
      diffutils
      findutils
      python3
      openssl
      libssh2
      zlib
    ];

    # Ensure dynamic libraries from the build closure are available
    system.extraDependencies = with pkgs; [ openssl libssh2 zlib ];

    nix.settings = {
      experimental-features = [ "nix-command" "flakes" ];
      trusted-users = [ "root" ];
      sandbox = false;
    };
  };

  testScript = ''
    def hash_dir(path):
        """Collect file sizes and sha256 hashes via SSH."""
        import shlex
        files = machine.succeed(f"find {path} -type f | sort").strip().splitlines()
        result = {}
        for fpath in files:
            if not fpath:
                continue
            rel = fpath.replace(path + "/", "", 1)
            q = shlex.quote(fpath)
            out = machine.succeed(f"stat -c '%s' {q}").strip()
            size = int(out)
            sha = machine.succeed(f"sha256sum {q}").strip().split()[0]
            result[rel] = {'size': size, 'sha256': sha}
        return result

    def fmt_size(n):
        for u in ('B', 'KB', 'MB', 'GB'):
            if n < 1024:
                return f"{n:.1f} {u}"
            n /= 1024
        return f"{n:.1f} TB"

    def dir_summary(d):
        files = len(d)
        total = sum(v.get('size', 0) for v in d.values())
        return f"{files} files, {fmt_size(total)}"

    def assert_dirs_equal(python_dst, rust_dst, label):
        machine.succeed("sync")
        py = hash_dir(python_dst)
        rs = hash_dir(rust_dst)

        py_keys = set(py.keys())
        rs_keys = set(rs.keys())

        only_py = py_keys - rs_keys
        only_rs = rs_keys - py_keys
        common = py_keys & rs_keys

        failures = []

        if only_py:
            failures.append(f"  Only in Python copy ({len(only_py)}): {sorted(only_py)[:5]}")
        if only_rs:
            failures.append(f"  Only in Rust copy ({len(only_rs)}): {sorted(only_rs)[:5]}")

        for key in sorted(common):
            p = py[key]
            r = rs[key]
            if 'error' in p or 'error' in r:
                continue
            if p['size'] != r['size']:
                failures.append(f"  Size mismatch: {key}: Python={p['size']} Rust={r['size']}")
                continue
            if p['sha256'] != r['sha256']:
                failures.append(f"  Content mismatch: {key}")
                failures.append(f"    Python SHA256: {p['sha256']}")
                failures.append(f"    Rust SHA256:   {r['sha256']}")

        if failures:
            msg = f"\nPARITY FAILURE ({label}):\n" + "\n".join(failures)
            msg += f"\n\nPython dst ({python_dst}): {dir_summary(py)}"
            msg += f"\nRust dst ({rust_dst}):   {dir_summary(rs)}"
            raise Exception(msg)

        print(f"  OK {label}: {dir_summary(py)}  both match")


    def run_python_copy(src, dst, extra_args=""):
        cmd = f"fast-copy {src} {dst} --no-verify {extra_args}"
        rc, out = machine.execute(cmd)
        print(out[:300])
        if rc != 0:
            raise Exception(f"Python copy failed (exit {rc}): {out[-300:]}")
        return out

    def run_rust_copy(src, dst, extra_args=""):
        cmd = f"fc {src} {dst} --no-verify {extra_args}"
        rc, out = machine.execute(cmd)
        print(out[:300])
        if rc != 0:
            raise Exception(f"Rust copy failed (exit {rc}): {out[-300:]}")
        return out


    # ── Setup ─────────────────────────────────────────────────────────
    machine.start()
    machine.wait_for_unit("network.target")

    machine.succeed("mkdir -p /data/source")
    machine.succeed("cp -r ${testData}/* /data/source/")

    machine.succeed("mkfs.ext4 /dev/vdb")
    machine.succeed("mkdir -p /data/dst")
    machine.succeed("mount /dev/vdb /data/dst")

    machine.succeed("test -f $(which fast-copy)")
    machine.succeed("test -f $(which fc)")
...
        _rc_rs, out_rs = machine.execute(f"fc /data/empty-src {dst_rs}/empty --no-dedup --no-verify 2>&1")
        print(f"Python empty: {out_py.strip()[-200:]}")
        print(f"Rust empty:   {out_rs.strip()[-200:]}")
        py_files = machine.succeed(f"find {dst_py}/empty -type f 2>/dev/null || true").strip()
        rs_files = machine.succeed(f"find {dst_rs}/empty -type f 2>/dev/null || true").strip()
        py_count = len(py_files.split()) if py_files else 0
        rs_count = len(rs_files.split()) if rs_files else 0
        if py_count != 0 or rs_count != 0:
            raise Exception(f"Empty dir: Python={py_count} files, Rust={rs_count} files (both should be 0)")
        print("  OK Empty directory: 0 files for both implementations")

    # ── Test 8: Force mode and large buffer ───────────────────────────
    with subtest("Force flag"):
        machine.succeed(f"mkdir -p {dst_py}/force {dst_rs}/force")
        run_python_copy(src, f"{dst_py}/force", "--no-dedup --force --buffer 8")
        run_rust_copy(src, f"{dst_rs}/force", "--no-dedup --force --buffer 8")
        assert_dirs_equal(f"{dst_py}/force", f"{dst_rs}/force", "Force mode")
        machine.succeed(f"rm -rf {dst_py}/force {dst_rs}/force")

    # ── Summary ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  ALL PARITY TESTS PASSED")
    print("  Python and Rust implementations produce identical results")
    print("=" * 60)
  '';
}
