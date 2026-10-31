"""Frozen diff and dependency scope controls; no Git/Cargo/product execution."""
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('lumen_impact_test',Path(__file__).parents[1]/'plan.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


def package(name,dependencies=()):
    return dict(name=name,directory='crates/'+name,features={},dependencies=list(dependencies))


class ImpactTests(unittest.TestCase):
    def test_delete_and_both_rename_sides_survive_without_filename_quoting_loss(self):
        raw='R095\0crates/a/old file.rs\0crates/b/new\nfile.rs\0D\0crates/a/deleted.rs\0'
        self.assertEqual(p.diff_paths(raw),['crates/a/deleted.rs','crates/a/old file.rs','crates/b/new\nfile.rs'])

    def test_unknown_truncated_and_unsafe_paths_fail_closed(self):
        for raw in ['M\0file.rs','R100\0only-old.rs\0','U\0x\0','R101\0a\0b\0',
                    'A\0/absolute\0','M\0a/../b\0','M\0.git/config\0','A\0\0']:
            with self.subTest(raw=raw),self.assertRaises(ValueError):p.diff_paths(raw)

    def test_frozen_commits_and_reverse_users_are_recorded_without_narrowing_execution(self):
        packages=[package('a'),package('b',['a']),package('other')]
        provenance=dict(actual_head='a'*40,actual_base='b'*40);calls=[]
        def read(argv,**kwargs):
            calls.append(argv);return 'M\0crates/a/src/lib.rs\0'
        result=p.impact(Path('/candidate'),packages,provenance,read)
        self.assertEqual(calls[0][-3:],['b'*40,'a'*40,'--'])
        self.assertIn('-z',calls[0]);self.assertIn('--find-renames',calls[0])
        self.assertEqual(result['affected_packages'],['a','b'])
        self.assertEqual(result['execution_selection'],'complete workspace')
        self.assertEqual(result['selected_unit_rows'],3)

    def test_unknown_and_deleted_package_paths_keep_complete_fallback(self):
        packages=[package('a'),package('b')]
        self.assertEqual(p.affected(packages,['crates/deleted/src/lib.rs']),['a','b'])
        self.assertEqual(p.affected(packages,[]),['a','b'])


if __name__=='__main__':unittest.main()
