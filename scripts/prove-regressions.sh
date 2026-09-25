#!/bin/sh
# Prove that the two regression tests can actually fail.
#
# A regression test that has never been seen to fail is a comment. This script
# reverts each fix in turn, runs only the test that pins it, records the
# failure, restores the source, and then runs the test again to show it passes.
# The transcript lands in results/regressions.txt so the claim is an artifact
# rather than a recollection.
#
# The source file is edited in place and restored from a backup on every exit
# path, including a failure or an interrupt. If the script is killed with
# SIGKILL the backup is still at raft/node.go.graft-backup.
#
# Usage: scripts/prove-regressions.sh [output-file]

set -eu

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

out=${1:-results/regressions.txt}
src=raft/node.go
backup=$src.graft-backup

if [ -e "$backup" ]; then
	echo "prove-regressions: $backup already exists; remove it and retry" >&2
	exit 1
fi

restore() {
	if [ -e "$backup" ]; then
		mv "$backup" "$src"
	fi
}
trap restore EXIT INT TERM

mkdir -p "$(dirname "$out")"

: > "$out"
{
	echo "Proof that the regression tests fail against the code they were written for"
	echo "=========================================================================="
	echo
	echo "Each fix is reverted in raft/node.go, the pinning test is run, and the"
	echo "source is restored. Reproduce with: scripts/prove-regressions.sh"
	echo
} >> "$out"

run_case() {
	label=$1
	revert=$2
	test_name=$3

	cp "$src" "$backup"
	# shellcheck disable=SC2086
	sed -i.bak "$revert" "$src"
	rm -f "$src.bak"

	{
		echo "------------------------------------------------------------------"
		echo "reverted fix: $label"
		echo "sed expression: $revert"
		echo "test: go test -run $test_name -v ./raft/"
		echo "expected: FAIL"
		echo
	} >> "$out"

	status=0
	go test -count=1 -run "$test_name" -v ./raft/ >> "$out" 2>&1 || status=$?

	{
		echo
		echo "observed exit status: $status"
		if [ "$status" -eq 0 ]; then
			echo "VERDICT: UNEXPECTED PASS -- the regression test no longer detects the bug"
		else
			echo "VERDICT: the regression test detected the bug, as intended"
		fi
		echo
	} >> "$out"

	restore

	{
		echo "------------------------------------------------------------------"
		echo "fix restored; test: go test -run $test_name ./raft/"
		echo "expected: PASS"
		echo
	} >> "$out"
	go test -count=1 -run "$test_name" ./raft/ >> "$out" 2>&1
	echo >> "$out"

	if [ "$status" -eq 0 ]; then
		echo "prove-regressions: $test_name passed with the fix reverted" >&2
		exit 1
	fi
}

run_case \
	"Storage.InstallSnapshot clears the whole local log (bug 1)" \
	's|n.cfg.Storage.InstallSnapshot(\*snap)|n.cfg.Storage.SaveSnapshot(*snap)|' \
	'TestInstallSnapshotSupersedesStaleSuffix'

run_case \
	"sendAppend copies the entries out of the log (bug 2)" \
	'/^\tents = append(\[\]Entry(nil), ents\.\.\.)$/d' \
	'TestOutboundAppendEntriesDoesNotAliasTheLog'

{
	echo "------------------------------------------------------------------"
	echo "Both fixes are in place and the tree is restored:"
	echo
	git status --short || true
	echo "(no output above means the working tree is unchanged)"
} >> "$out"

echo "wrote $out"
