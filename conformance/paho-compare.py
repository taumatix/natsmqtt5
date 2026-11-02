#!/usr/bin/env python3
"""Compare the Paho MQTT 5 suite's per-test results with conformance/paho-expected.tsv.

usage: paho-compare.py EXPECTED.tsv SUITE.py CONFIG=OUTPUT.txt [CONFIG=OUTPUT.txt ...]
       paho-compare.py --write EXPECTED.tsv ...   (print a fresh table, reasons left blank)

OUTPUT is the combined stdout and stderr of `python3 client_test5.py -v`. unittest prints the
progress lines interleaved with the suite's own logging, so the progress lines are not parsed; a
test failed if it is named in the `FAIL:` or `ERROR:` block at the end, passed if the run reached
"Ran N tests" with N equal to the number of tests the suite defines and the test is not named
there. A run that did not finish fails the comparison.

EXPECTED.tsv: a header row `test<TAB>CONFIG<TAB>...<TAB>reason`, then one row per test with
PASS or FAIL per configuration, or FLAKY for a test whose result the suite itself leaves to a
race (either result is accepted, and the reason says why). Any other difference, in either
direction, exits non-zero.
"""
import re
import sys


def suite_tests(path):
    return re.findall(r"^\s+def (test_\w+)\(self", open(path, encoding="utf-8").read(), re.M)


def results(output_path, tests):
    text = open(output_path, encoding="utf-8", errors="replace").read()
    ran = re.search(r"^Ran (\d+) tests? in ", text, re.M)
    if not ran:
        raise SystemExit(f"{output_path}: the suite did not finish (no 'Ran N tests' line)")
    if int(ran.group(1)) != len(tests):
        raise SystemExit(f"{output_path}: ran {ran.group(1)} tests, the suite defines {len(tests)}")
    failed = set(re.findall(r"^(?:FAIL|ERROR): (test_\w+) ", text, re.M))
    unknown = failed - set(tests)
    if unknown:
        raise SystemExit(f"{output_path}: failures named for unknown tests {sorted(unknown)}")
    return {t: ("FAIL" if t in failed else "PASS") for t in tests}


def main(argv):
    mode = argv[0] if argv and argv[0].startswith("--") else ""
    write = mode == "--write"
    if mode:
        argv = argv[1:]
    expected_path, suite_path, *runs = argv
    tests = suite_tests(suite_path)
    configs, actual = [], {}
    for run in runs:
        name, _, out = run.partition("=")
        configs.append(name)
        actual[name] = results(out, tests)
    for name in configs:
        n = sum(1 for v in actual[name].values() if v == "PASS")
        print(f"{name}: {n} passed, {len(tests) - n} failed of {len(tests)}")
    if write:
        print("test\t" + "\t".join(configs) + "\treason")
        for t in tests:
            print(t + "\t" + "\t".join(actual[n][t] for n in configs) + "\t")
        return 0

    rows = [l.rstrip("\n").split("\t") for l in open(expected_path, encoding="utf-8") if l.strip() and not l.startswith("#")]
    header, rows = rows[0], rows[1:]
    if header[0] != "test" or header[-1] != "reason" or header[1:-1] != configs:
        raise SystemExit(f"{expected_path}: header {header} does not match the configurations {configs}")
    expected = {r[0]: dict(zip(configs, r[1:-1])) for r in rows}
    bad = []
    for t in sorted(set(expected) | set(tests)):
        if t not in expected:
            bad.append(f"{t}: in the suite, missing from {expected_path}")
        elif t not in tests:
            bad.append(f"{t}: in {expected_path}, not in the suite")
        else:
            for name in configs:
                if expected[t][name] not in (actual[name][t], "FLAKY"):
                    hint = "a new failure" if actual[name][t] == "FAIL" else "now passes, update the file"
                    bad.append(f"{t} [{name}]: expected {expected[t][name]}, got {actual[name][t]} ({hint})")
    for b in bad:
        print("DIFFERENCE", b)
    if bad:
        return 1
    print(f"all {len(tests)} tests match {expected_path} in {len(configs)} configurations")
    return 0


sys.exit(main(sys.argv[1:]))
