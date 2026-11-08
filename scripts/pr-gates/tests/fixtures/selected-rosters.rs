#[test]
fn ordinary() {}

#[test]
#[ignore]
fn another_ignored() {}

mod perf_gate {
    #[test]
    #[ignore]
    fn first() {}

    #[test]
    #[ignore]
    fn second() {}

    mod durable_workload {
        #[test]
        #[ignore]
        fn release_only() {}
    }
}
