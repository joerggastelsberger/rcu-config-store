fn main() {
    // Declare `--cfg loom` as expected so `unexpected_cfgs` stays useful
    // for catching real cfg typos.
    println!("cargo::rustc-check-cfg=cfg(loom)");
}
