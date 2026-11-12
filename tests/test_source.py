import json
import unittest
from pathlib import Path
from reader import inspect
from source import seed
import tempfile


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).parents[1] / "fixtures/source.json").read_text(encoding="utf-8"))
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        from source import seed
        seed(self.root, self.fixture)

    def test_open_tail_not_read(self):
        from reader import read_closed_lines
        self.assertEqual(len(read_closed_lines(self.root/'gateway.log')),1)

    def test_real_rename_changes_path_identity(self):
        from source import inventory,rotate
        before=inventory(self.root)[0]['identity']
        rotate(self.root,self.fixture['new_file'])
        entries={entry['path']:entry for entry in inventory(self.root)}
        self.assertEqual(entries['gateway.log.1']['identity'],before)
        self.assertNotEqual(entries['gateway.log']['identity'],before)

    def test_append_completes_record(self):
        from source import append
        from reader import read_closed_lines
        append(self.root,self.fixture['append'])
        self.assertEqual(read_closed_lines(self.root/'gateway.log')[1]['seq'],2)

    def test_truncate_keeps_inode(self):
        from source import inventory,truncate
        before=inventory(self.root)[0]['identity']
        truncate(self.root,self.fixture['new_file'])
        self.assertEqual(inventory(self.root)[0]['identity'],before)

    def test_archive_bytes(self):
        import gzip
        from source import rotate,compress_archive
        rotate(self.root,self.fixture['new_file'])
        compress_archive(self.root)
        self.assertEqual(gzip.decompress((self.root/'gateway.log.1.gz').read_bytes()),
                         self.fixture['files']['gateway.log'].encode())


if __name__ == "__main__":
    unittest.main()
