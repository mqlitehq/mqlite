"""Exact tracked-source archive and byte verification helpers."""
import hashlib, io, os, pathlib, subprocess, tarfile

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def source(root):
    return {p.relative_to(root).as_posix(): sha(p) for p in root.rglob('*') if p.is_file()}

def checkout(repo, rev, dest):
    data = subprocess.check_output(['git', '-C', str(repo), 'archive', '--format=tar', rev])
    dest.mkdir()
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        for m in tar:
            n = pathlib.PurePosixPath(m.name)
            if n.is_absolute() or '..' in n.parts or (not (m.isdir() or m.isfile())):
                raise ValueError('nonregular source')
            if m.isdir():
                (dest / m.name).mkdir(parents=True, exist_ok=True)
            else:
                p = dest / m.name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(tar.extractfile(m).read())
    mapping = source(dest)
    entries = subprocess.check_output(['git', '-C', str(repo), 'ls-tree', '-r', '-z', rev]).split(b'\x00')
    expected = set()
    object_format = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--show-object-format'], text=True).strip()
    assert object_format in ('sha1', 'sha256')
    for entry in entries:
        if not entry:
            continue
        header, name = entry.split(b'\t', 1)
        mode, kind, checksum = header.decode('ascii').split()
        relative = os.fsdecode(name)
        assert kind == 'blob' and mode in ('100644', '100755')
        data = (dest / relative).read_bytes()
        actual = hashlib.new(object_format, b'blob ' + str(len(data)).encode() + b'\x00' + data).hexdigest()
        assert actual == checksum
        expected.add(relative)
    assert expected == set(mapping)
    return mapping

