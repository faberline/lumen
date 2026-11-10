"""Actual Rust child stdout must not corrupt strict libtest execution proof."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('generator_harness_plan',ROOT/'scripts/pr-gates/plan.py')
plan=importlib.util.module_from_spec(spec);spec.loader.exec_module(plan)


class GeneratorHarnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=tempfile.TemporaryDirectory(prefix='lumen-generator-harness-')
        root=Path(cls.directory.name)
        source=(ROOT/'crates/lumen/tests/it/spec_gen_e2e.rs').read_text()
        start=source.index('fn run_generator(');end=source.index('\npub(super) fn run_generated_python',start)
        helper=source[start:end]
        fixture=root/'fixture.rs'
        fixture.write_text('fn main() { println!("generated isolated/client.py"); eprintln!("isolated child stderr"); if std::env::args().any(|a| a == "fail") { std::process::exit(7); } }')
        cls.child=root/'child'
        subprocess.run(['rustc','--edition=2021',str(fixture),'-o',str(cls.child)],check=True,capture_output=True,text=True)
        cls.binaries={}
        for mode in ('captured','inherited'):
            body=('let result=run_generator(Command::new(child).arg(argument)); assert!(result.status.success(), "child failed: {} {}", String::from_utf8_lossy(&result.stdout), String::from_utf8_lossy(&result.stderr));'
                  if mode=='captured'else'let result=Command::new(child).arg(argument).status().unwrap(); assert!(result.success());')
            harness=root/(mode+'.rs')
            harness.write_text('use std::process::Command;\n'+helper+'\n#[test] fn generation() { let child=std::env::var("ISOLATED_CHILD").unwrap(); let argument=std::env::var("ISOLATED_ARGUMENT").unwrap(); '+body+' }')
            binary=root/mode
            subprocess.run(['rustc','--edition=2021','--test',str(harness),'-o',str(binary)],check=True,capture_output=True,text=True)
            cls.binaries[mode]=binary

    @classmethod
    def tearDownClass(cls):cls.directory.cleanup()

    def run_fixture(self,mode,argument='pass'):
        return subprocess.run([str(self.binaries[mode]),'--test-threads=1'],text=True,capture_output=True,
            env=dict(os.environ,ISOLATED_CHILD=str(self.child),ISOLATED_ARGUMENT=argument))

    def test_actual_capture_keeps_the_complete_exact_roster(self):
        result=self.run_fixture('captured');self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(plan._libtest_blocks(result.stdout,1)[0]['roster'],['generation'])

    def test_original_inherited_stdout_is_rejected_without_relaxing_parser(self):
        result=self.run_fixture('inherited');self.assertEqual(result.returncode,0,result.stderr)
        with self.assertRaisesRegex(ValueError,'nonpassing test row'):plan._libtest_blocks(result.stdout,1)

    def test_failed_child_retains_exit_failure_and_both_diagnostic_streams(self):
        result=self.run_fixture('captured','fail');self.assertNotEqual(result.returncode,0)
        self.assertIn('generated isolated/client.py',result.stdout+result.stderr)
        self.assertIn('isolated child stderr',result.stdout+result.stderr)
        with self.assertRaises(ValueError):plan._libtest_blocks(result.stdout,1)


if __name__=='__main__':unittest.main()
