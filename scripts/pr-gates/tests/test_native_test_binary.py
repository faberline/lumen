"""Exercise the actual nested-harness helper using isolated Rust fixtures."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[3]


class NativeTestBinaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=tempfile.TemporaryDirectory(prefix='lumen-nested-harness-control-')
        root=Path(cls.directory.name)
        source=(ROOT/'crates/lumen/tests/it/support/native_contracts.rs').read_text()
        start=source.index('pub fn test_binary(');end=source.index('\nstruct Proxy',start)
        # The only substitution is a no-op JSON receipt sink. All process,
        # discovery, output and nonzero assertions use the actual source.
        helper=root/'helper.rs'
        helper.write_text('use std::process::Command;\n#[macro_export] macro_rules! json { ($($tokens:tt)*) => { () } }\nmod serde_json { pub use crate::json; }\nfn evidence(_: &str, _: &str, _: ()) {}\n'+source[start:end]+'\nfn main() { test_binary("ISOLATED_HARNESS_FIXTURE", "measurement"); }\n')
        cls.helper=root/'helper'
        subprocess.run(['rustc','--edition=2021',str(helper),'-o',str(cls.helper)],check=True,capture_output=True,text=True)
        cls.binaries={}
        for name,body in [('measurement','println!("actual isolated measurement: 42");'),('different','')]:
            path=root/(name+'.rs');path.write_text('#[test] fn '+name+'() { '+body+' }\n')
            binary=root/name
            subprocess.run(['rustc','--edition=2021','--test',str(path),'-o',str(binary)],check=True,capture_output=True,text=True)
            cls.binaries[name]=binary

    @classmethod
    def tearDownClass(cls):cls.directory.cleanup()

    def test_measurement_output_does_not_corrupt_exact_execution_proof(self):
        result=subprocess.run([str(self.helper)],env=dict(os.environ,ISOLATED_HARNESS_FIXTURE=str(self.binaries['measurement'])),text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('actual isolated measurement: 42',result.stderr)
        self.assertIn('1 passed; 0 failed; 0 ignored;',result.stderr)

    def test_missing_exact_name_cannot_be_zero_test_success(self):
        result=subprocess.run([str(self.helper)],env=dict(os.environ,ISOLATED_HARNESS_FIXTURE=str(self.binaries['different'])),text=True,capture_output=True)
        self.assertNotEqual(result.returncode,0)


if __name__=='__main__':unittest.main()
