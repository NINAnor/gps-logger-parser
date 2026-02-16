import re


def stream_starts_with(stream, text):
    position = stream.tell()
    chunk = stream.read(len(text))
    stream.seek(position)
    return chunk == text


def stream_chunk_contains(stream, length, text):
    position = stream.tell()
    chunk = stream.read(length)
    stream.seek(position)
    return text in chunk


def stream_chunk_match(stream, length, text):
    position = stream.tell()
    chunk = stream.read(length)
    stream.seek(position)
    return re.search(text, chunk)


def read_csv(data, separator=",", newline="\n", trailing=""):
    for line in data.split(newline):
        if not line:
            break  # EOF?
        if len(trailing) > 0:
            if not line.endswith(trailing):
                raise ValueError(f"Line does not end with expected trailing: {line}")
            line = line[: -len(trailing)]
        yield line.split(separator)
