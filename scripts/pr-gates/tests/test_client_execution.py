"""Pure receipts and installer controls; no generated client or product runs."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
def load(name,file):
    spec=importlib.util.spec_from_file_location(name,ROOT/'scripts/pr-gates'/file)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
c=load('client_receipt_controls','contracts.py')
tools=load('client_installer_controls','install-client-tools.py')


def receipt(language):
    return dict(schema='LUMEN_GENERATED_CLIENT_CRUD_V1',language=language,result='pass',exit_code=0,
        generation_mode='legacy_untargeted',requirements_source='separate_existing_explicit_target_reference',
        manifest=dict(language=language,target='fixed',compiler='fixed',minimum_version='1',language_standard='1'),
        checks=c.CLIENT_CHECKS,stdout_sha256='1'*64,stderr_sha256='2'*64)


class ClientExecutionTests(unittest.TestCase):
    def write(self,root):
        for language in ('python','typescript','rust'):
            (root/(language+'.json')).write_text(json.dumps(receipt(language)))

    def test_each_language_is_required_with_exact_crud_assertions(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);self.write(root)
            self.assertEqual([r['language']for r in c.client_receipts(root)],['python','rust','typescript'])
            for language in ('python','typescript','rust'):
                path=root/(language+'.json');prior=path.read_text();path.unlink()
                with self.assertRaisesRegex(ValueError,'missing'):c.client_receipts(root)
                path.write_text(prior)

    def test_failed_duplicate_and_partial_language_proof_refuse(self):
        for key,value in [('result','skip'),('exit_code',1),('exit_code',False),('language','rust'),('checks',c.CLIENT_CHECKS[:-1]),('stdout_sha256','x'*64)]:
            with self.subTest(key=key,value=value),tempfile.TemporaryDirectory()as directory:
                root=Path(directory);self.write(root);row=receipt('python');row[key]=value
                (root/'python.json').write_text(json.dumps(row))
                with self.assertRaisesRegex(ValueError,'invalid'):c.client_receipts(root)
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);self.write(root);(root/'python-copy.json').write_text('{}')
            with self.assertRaisesRegex(ValueError,'extra'):c.client_receipts(root)

    def test_changed_compiler_bytes_refuse(self):
        with self.assertRaisesRegex(ValueError,'integrity'):tools.verified(b'changed')

    def test_install_is_hosted_only(self):
        with patch.dict(tools.os.environ,{'GITHUB_ACTIONS':'false'}),patch.object(tools.subprocess,'run')as run:
            with self.assertRaisesRegex(ValueError,'hosted CI'):tools.main(['--dest','/not-used'])
            run.assert_not_called()

    def test_archive_escape_and_links_refuse_before_write(self):
        for name,kind in [('../outside',tarfile.REGTYPE),('package/lib/link',tarfile.SYMTYPE),('/absolute',tarfile.REGTYPE)]:
            stream=io.BytesIO()
            with tarfile.open(fileobj=stream,mode='w:gz')as archive:
                entry=tarfile.TarInfo(name);entry.type=kind;entry.size=0;archive.addfile(entry)
            with tempfile.TemporaryDirectory()as directory,self.assertRaisesRegex(ValueError,'unsafe'):
                tools.unpack(stream.getvalue(),Path(directory)/'output')


if __name__=='__main__':unittest.main()
