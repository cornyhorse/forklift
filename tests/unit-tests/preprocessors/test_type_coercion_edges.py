from forklift.preprocessors.type_coercion import TypeCoercion

def test_type_coercion_happy_and_errors():
    tc = TypeCoercion(types={
        "active": {"type":"boolean"},
        "signup_date": {"type":"string", "format":"date"},
        "amount": {"type":"number"},
        "name": {"type":"string"},
    })
    ok = {"active":"YES","signup_date":"2024-03-01","amount":"10.50","name":"Amy"}
    rr_ok = tc.process(ok)   # <-- was tc(ok)
    assert rr_ok["row"]["active"] is True
    assert rr_ok["error"] is None

    bad = {"active":"no","signup_date":"not-a-date","amount":"xx","name":"Bob"}
    rr_bad = tc.process(bad)
    assert rr_bad["error"] is not None