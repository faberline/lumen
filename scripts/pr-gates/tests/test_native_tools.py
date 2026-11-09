"""Native tool installation must reject changed or unsafe artifacts."""
import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('native_tool_controls',ROOT/'scripts/pr-gates/install-native-tools.py')
tools=importlib.util.module_from_spec(spec);spec.loader.exec_module(tools)


class NativeToolTests(unittest.TestCase):
    def test_changed_download_digest_refuses(self):
        with patch.object(tools.urllib.request,'urlopen',return_value=io.BytesIO(b'changed')):
            with self.assertRaisesRegex(ValueError,'digest mismatch'):tools.verified_download('https://example.invalid/tool',hashlib.sha256(b'approved').hexdigest())

    def test_only_single_regular_expected_archive_member_is_accepted(self):
        for name,kind in [('kustomize',tarfile.REGTYPE),('../escape',tarfile.REGTYPE),('kustomize',tarfile.SYMTYPE)]:
            with self.subTest(name=name,kind=kind):
                data=io.BytesIO()
                with tarfile.open(fileobj=data,mode='w:gz')as archive:
                    member=tarfile.TarInfo(name);member.type=kind
                    member.size=4 if kind==tarfile.REGTYPE else 0
                    archive.addfile(member,io.BytesIO(b'tool')if member.size else None)
                if name=='kustomize'and kind==tarfile.REGTYPE:self.assertEqual(tools.executable('kustomize',data.getvalue()),b'tool')
                else:
                    with self.assertRaisesRegex(ValueError,'archive layout'):tools.executable('kustomize',data.getvalue())

    def test_local_control_cannot_install_runtime(self):
        with patch.dict(tools.os.environ,{'GITHUB_ACTIONS':'false'}),patch.object(tools,'verified_download')as download:
            with self.assertRaisesRegex(ValueError,'hosted CI'):tools.main(['--dest','/not-used'])
            download.assert_not_called()


if __name__=='__main__':unittest.main()
