"""External execution must bind complete real rosters to named owners."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('external_controls',ROOT/'scripts/pr-gates/external.py')
external=importlib.util.module_from_spec(spec);spec.loader.exec_module(external)


class ExternalTests(unittest.TestCase):
    def test_current_named_owners_are_disjoint_and_nonzero(self):
        functional=external.selected(ROOT,'functional')
        performance=external.selected(ROOT,'performance')
        self.assertTrue(functional and performance)
        self.assertFalse(functional & performance)
        self.assertIn(('perf_gate_vs_db','competitive_perf_gate'),performance)
        self.assertIn(('disk_scale_proof','scale_proof_reopen_rss_is_bounded'),performance)
        self.assertEqual(sum(name.startswith('docker_named_volume::')for _,name in functional),2)

    def test_ambient_workload_reduction_refuses_before_build(self):
        for key in ('RIG_VAR_KEYSPACE','CARGO_PROFILE_RELEASE_OPT_LEVEL','LUMEN_GATE_N','LUMEN_SCALE_N'):
            with self.subTest(key=key),patch.dict(external.os.environ,{key:'1'}),patch.object(external,'built_binaries')as build:
                with self.assertRaisesRegex(ValueError,'overrides'):external.run(ROOT,'performance',{})
                build.assert_not_called()

    def test_missing_owner_cannot_be_empty_success(self):
        with patch.object(external.contracts,'ignored_contracts',return_value=[]):
            with self.assertRaisesRegex(ValueError,'zero selected'):external.selected(ROOT,'functional')

    def test_timeout_kills_and_reaps_its_owned_process_group(self):
        child=Mock(pid=12345,returncode=-9)
        child.communicate.side_effect=[subprocess.TimeoutExpired(['fixture'],1),('partial output','partial error')]
        with patch.object(external.subprocess,'Popen',return_value=child)as popen,patch.object(external.os,'killpg')as kill:
            with self.assertRaises(subprocess.TimeoutExpired)as error:external.invoke(['fixture'],timeout=1)
            self.assertTrue(popen.call_args.kwargs['start_new_session'])
            kill.assert_called_once_with(12345,external.signal.SIGKILL)
            self.assertEqual(child.communicate.call_count,2)
            self.assertEqual(error.exception.output,'partial output')

    def test_real_harness_graceful_skip_is_visible_and_refused(self):
        with tempfile.TemporaryDirectory(prefix='lumen-direct-output-control-')as directory:
            root=Path(directory);source=root/'direct.rs';binary=root/'direct'
            source.write_text('#[test] fn skipped() { eprintln!("[scale_proof] memory_stats unavailable - SKIPPING"); }\n#[test] fn measured() { println!("actual isolated measurement: 42"); }\n')
            subprocess.run(['rustc','--test','--edition=2021',str(source),'-o',str(binary)],check=True,capture_output=True,text=True)
            full=external.discover(str(binary))
            for name in ('measured','skipped'):
                result=external.invoke(external.test_command(str(binary),'disk_scale_proof',name))
                self.assertEqual(result.returncode,0)
                proof=external.measure.selected_evidence(result.stdout,full,{name})
                self.assertEqual(proof['passed'],1)
                if name=='skipped':
                    with self.assertRaisesRegex(ValueError,'skipped'):external.validate_direct_output('disk_scale_proof',result.stdout,result.stderr)
                else:
                    external.validate_direct_output('disk_scale_proof',result.stdout,result.stderr)
                    self.assertIn('actual isolated measurement: 42',result.stdout)


if __name__=='__main__':unittest.main()
