import pytest
from ctlog.log import Log, encode_entry, decode_entry

CERT = {"domain": "minerva.edu", "issuer": "Let's Encrypt"}


def test_new_log_is_empty():
    log = Log()
    assert len(log) == 0


def test_get_returns_the_entry():
    log = Log()
    index = log.append_entry(CERT)
    assert log.get_entry_at(index) == CERT


# Runs the test with several invalid inputs
@pytest.mark.parametrize("invalid_index", [-1, 1, 1020])
def test_out_of_range_index(invalid_index):
    log = Log()
    log.append_entry(CERT)
    with pytest.raises(IndexError):
        log.get_entry_at(invalid_index)


def test_append_returns_sequential_indices():
    log = Log()
    indices = [log.append_entry({"domain": f"site{i}.com"}) for i in range(5)]
    assert indices == list(range(5))
    assert len(log) == 5
    entries = [log.get_entry_at(i) for i in range(5)]
    assert entries == [{"domain": f"site{i}.com"} for i in range(5)]


def test_changing_original_entry_does_not_change_log():
    log = Log()
    entry = CERT.copy()  # making a shallow copy so we don't change the original object
    index = log.append_entry(entry)
    entry["issuer"] = "yalahwy"
    assert log.get_entry_at(index) == CERT


# Deliberate: the Log is a plain append-only list and doesn't deduplicate.
def test_duplicate_entries_are_stored_separately():
    log = Log()
    first = log.append_entry(CERT)
    second = log.append_entry(CERT)
    assert (first, second) == (0, 1)
    assert len(log) == 2
    assert log.get_entry_at(0) == log.get_entry_at(1) == CERT


def test_changing_returned_dict_does_not_change_log():
    log = Log()
    index = log.append_entry(CERT)
    returned = log.get_entry_at(index)
    returned["issuer"] = "yalahwy"
    assert log.get_entry_at(index) == CERT


@pytest.mark.parametrize("invalid_entry", ["minerva.edu", ["minerva.edu"], None, 218])
def test_non_dict_entry_raises_type_error(invalid_entry):
    log = Log()
    with pytest.raises(TypeError):
        log.append_entry(invalid_entry)
    assert len(log) == 0


def test_failed_append_does_not_use_up_an_index():
    log = Log()
    first = log.append_entry({"domain": "minerva.edu"})
    with pytest.raises(TypeError):
        log.append_entry("not a dict")
    second = log.append_entry({"domain": "forum.minerva.edu"})
    assert (first, second) == (0, 1)
    assert len(log) == 2
    assert log.get_entry_at(1) == {"domain": "forum.minerva.edu"}


@pytest.mark.parametrize(
    "invalid_entry, expected_error",
    [
        ({"a": (1, 2)}, ValueError),  # tuple changes to a list
        ({1: "x"}, ValueError),  # int key changes to a string
        ({"a": {1, 2}}, TypeError),  # json can't serialize a set
    ],
)
def test_entries_json_changes_are_rejected(invalid_entry, expected_error):
    log = Log()
    with pytest.raises(expected_error):
        log.append_entry(invalid_entry)
    assert len(log) == 0


def test_key_order_gives_identical_encoding():
    first = {"domain": "minerva.edu", "issuer": "Let's Encrypt"}
    second = {"issuer": "Let's Encrypt", "domain": "minerva.edu"}
    assert encode_entry(first) == encode_entry(second)


def test_decoding_gives_original_entry():
    encoded_entry = encode_entry(CERT)
    assert decode_entry(encoded_entry) == CERT
