import json
from pipeline.events import make_event, parse
from pipeline.sink import Sink


def ev(i, type_="page_view", user="u1", amount=0.0, ts=1_700_000_000.0):
    return {"event_id": f"e{i}", "user_id": user, "type": type_, "ts": ts, "amount": amount}


def test_generated_events_parse():
    for _ in range(200):
        e = make_event()
        assert parse(json.dumps(e).encode()) == e


def test_parse_rejects_bad_payloads():
    for raw in [b"not json", b"{}", json.dumps(ev(1, type_="hack")).encode(),
                json.dumps(ev(1, amount=-5)).encode(), json.dumps({**ev(1), "ts": "x"}).encode()]:
        assert parse(raw) is None


def test_sink_dedupes_redelivery_and_dead_letters():
    s = Sink()
    s.write([ev(1), ev(2)])
    s.write([ev(2), ev(3)], bad=[b"garbage"])  # e2 redelivered after a consumer restart
    assert s.query("select count(*) from raw_events") == [(3,)]
    assert s.query("select payload from dead_letter") == [("garbage",)]


def test_models():
    s = Sink()
    s.write([ev(1, user="a"), ev(2, user="b"), ev(3, "add_to_cart", "a"), ev(4, "checkout", "a"),
             ev(5, "purchase", "a", 120.0), ev(6, "purchase", "a", 30.0)])
    assert s.build_models() == ["events_per_minute", "funnel", "top_customers"]
    funnel = s.query("select type, users, conversion_from_prev from funnel")
    assert funnel == [("page_view", 2, None), ("add_to_cart", 1, 0.5), ("checkout", 1, 1.0), ("purchase", 1, 1.0)]
    assert s.query("select user_id, purchases, revenue from top_customers") == [("a", 2, 150.0)]
    assert s.query("select sum(events) from events_per_minute") == [(6,)]
