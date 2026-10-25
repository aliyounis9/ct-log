import json


def encode_entry(entry):
    if not isinstance(entry, dict):
        raise TypeError("Entry object has to be a dictionary.")

    serialized_entry = json.dumps(
        entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )

    deserialized_entry = json.loads(serialized_entry)

    if deserialized_entry != entry:
        raise ValueError(
            f"Entry changes when converted to JSON: {entry} became {deserialized_entry}"
        )

    encoded_entry = serialized_entry.encode("utf-8")

    return encoded_entry


def decode_entry(encoded_object):
    return json.loads(encoded_object.decode("utf-8"))


class Log:
    def __init__(self):
        self._entries = []

    def append_entry(self, entry):
        index = len(self._entries)
        encoded_entry = encode_entry(entry)
        self._entries.append(encoded_entry)
        return index

    def get_entry_at(self, index):
        if index >= len(self._entries) or index < 0:
            raise IndexError(
                f"Index {index} out of range for log size of {len(self._entries)}"
            )
        return decode_entry(self._entries[index])

    def __len__(self):
        return len(self._entries)