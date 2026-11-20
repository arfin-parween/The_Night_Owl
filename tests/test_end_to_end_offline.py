"""Offline end-to-end rehearsal of the whole demo flow against the in-memory fake.
Run: python -m tests.test_end_to_end_offline   (from the project root)"""
import sys

from tests import fake_opensearch

fake = fake_opensearch.install()

from nightowl import cli, fixes, pipeline, scenarios, settings, setup_os, tools  # noqa: E402
import tempfile  # noqa: E402
from pathlib import Path  # noqa: E402

# Keep the test's "no ML model" state out of the real .nightowl_state.json, otherwise running the
# test silently switches the live demo to BM25-only search.
settings.STATE_FILE = Path(tempfile.mkdtemp(prefix="nightowl-test-")) / "state.json"
settings.save_state(model_id=None)
setup_os.create_indices(with_vectors=False, reset=True)
setup_os.seed(reset=True)
scenarios.reset()
base = pipeline.run(trigger="setup")
assert base["status"] == "SUCCESS", base

def banner(t):
    print("\n" + "=" * 20 + f" {t} " + "=" * 20)

for scenario, fix_type, params in [
    ("schema_drift", "add_column_alias", {"from": "order_value_inr", "to": "order_amount"}),
    ("bad_data", "enable_quarantine", {}),
    ("missing_partition", "rerun_only", {}),
]:
    banner(scenario.upper())
    cli.main(["reset"])  # what the presenter runs between scenarios
    scenarios.break_pipeline(scenario, with_injection=(scenario == "bad_data"))
    run = pipeline.run()
    assert run["status"] == "FAILED", run
    for fn in (tools.status, lambda: tools.failures(24), lambda: tools.timeline("latest"),
               lambda: tools.schema_diff("latest"), lambda: tools.changes(6),
               lambda: tools.logs_around("latest"), lambda: tools.similar(run["error_message"]),
               lambda: tools.impact(), lambda: tools.search_logs("store_id null")):
        print(fn(), "\n")
    print(tools.open_incident("latest", f"{scenario} test", "root cause text", "evidence text", fix_type, params))
    print(tools.pending())
    inc_id = tools.pending().splitlines()[1].split("|")[0].strip("- ").strip()
    # guard rails
    for bad in ("", "agent", "OpenClaw"):
        try:
            fixes.apply(inc_id, bad)
            raise AssertionError("apply should refuse non-human approver")
        except SystemExit as e:
            print("guard OK:", e)
    if scenario == "missing_partition":
        out = fixes.apply(inc_id, "presenter")
        print(out)
        assert "FAILED again" in out
        scenarios.deliver_late_partition()
    out = fixes.apply(inc_id, "presenter")
    print(out)
    assert "SUCCEEDED" in out, out
    assert tools.show_incident(inc_id)["status"] == "resolved"
    print(tools.similar(run["error_message"]))

banner("VALIDATION")
for bad_params in ({"from": "x; rm -rf /", "to": "order_amount"}, {"from": "a", "to": "not_a_contract_col"}):
    try:
        tools.validate_fix("add_column_alias", bad_params)
        raise AssertionError("should reject")
    except SystemExit as e:
        print("validation OK:", e)

cli.main(["status"])
print("\nALL OFFLINE CHECKS PASSED")
sys.exit(0)
