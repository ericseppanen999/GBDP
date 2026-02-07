import json

from gbdp.bronze.writer import BronzeWriter, RawPayload


def test_bronze_write_idempotent(tmp_path):
    writer = BronzeWriter(root=tmp_path)
    payload = RawPayload(
        source="test",
        entity="entity",
        dt="2024-01-01",
        url="http://example.com",
        params={},
        status_code=200,
        fetched_at_utc="2024-01-01T00:00:00Z",
        checksum="abc123",
        content_type="application/json",
        body_text=json.dumps({"a": 1}),
    )
    p1 = writer.write_raw(payload, force=False)
    p2 = writer.write_raw(payload, force=False)
    assert p1 == p2
    assert p1.exists()
