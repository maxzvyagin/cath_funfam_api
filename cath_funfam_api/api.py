import requests
from pathlib import Path
import os
import stat
import subprocess
from argparse import ArgumentParser
import json
import time
import pandas as pd

CATH_SUBMISSION_SCRIPT = """#!/usr/bin/perl
use strict;
use warnings;
use LWP::UserAgent;
 
my $ua = LWP::UserAgent->new;
$ua->timeout(10);
$ua->default_header( 'Accept' => 'application/json' );
 
my $url = 'http://www.cathdb.info/search/by_funfhmmer';
my %data = ();
$data{{fasta}} = <<'_PARAM';
{}
_PARAM
 
 
 
my $response = $ua->post( $url , \%data );
 
if ( $response->is_success ) {{
    print $response->decoded_content;
}}
else {{
    die $response->status_line;
}}
"""


def submit_sequence(seq: str, tmp_dir: Path = Path("/tmp"), verbose=False):
    """Creates a temporary PERL script, and runs it to submit it to the CATH search, returns the task id. Note that
    this function requires the ability to run a PERL script (haven't been able to get requests version to work)."""
    filled_script = CATH_SUBMISSION_SCRIPT.format(seq)
    tmp_script = tmp_dir / "cath_submission.pl"
    with open(tmp_script, "w") as f:
        f.write(filled_script)
    # chmod to be executable
    st = os.stat(tmp_script)
    os.chmod(tmp_script, st.st_mode | stat.S_IEXEC)
    # save current directory to chdir back to it once script has been run
    original_dir = os.getcwd()
    # now run the script to submit it, need to parse the output in order to get the task id and return it
    os.chdir(tmp_dir)
    submission_result = subprocess.run(
        ["./cath_submission.pl"], text=True, capture_output=True
    )
    os.chdir(original_dir)
    # parse the output of the perl script
    output = submission_result.stdout.strip()
    output_dict = json.loads(output)
    task_id = output_dict["task_id"]
    if verbose:
        print("Task ID: ", task_id)
    return task_id


def check_sequence_status(task_id: str):
    """Check if a task is completed based on the given task id"""
    headers = {
        "Accept": "application/json",
    }

    response = requests.get(
        "http://www.cathdb.info/search/by_funfhmmer/check/{}".format(task_id),
        headers=headers,
    )
    output = response.json()
    return bool(output["success"])


def get_sequence_results(task_id: str) -> (pd.DataFrame, pd.DataFrame):
    """Once a task is completed, retieve the results and return as pandas DataFrame"""
    headers = {
        "Accept": "application/json",
    }

    response = requests.get(
        "http://www.cathdb.info/search/by_funfhmmer/results/{}".format(task_id),
        headers=headers,
    )
    output = response.json()
    # retrieve scan results - there's the full scan and the resolved scan
    scan_results = output["funfam_scan"]["results"][0]["hits"]
    scan_df = pd.DataFrame(scan_results)
    resolved_scan_results = output["funfam_resolved_scan"]["results"][0]["hits"]
    resolved_scan_df = pd.DataFrame(resolved_scan_results)
    return scan_df, resolved_scan_df


def run_cath_workflow(
    seq: str, tmp_dir: Path = Path("/tmp"), verbose=False
) -> (pd.DataFrame, pd.DataFrame):
    """Run all three helper functions and return the dataframes"""
    task_id = submit_sequence(seq, tmp_dir=tmp_dir, verbose=verbose)
    # wait for the task to complete before retrieving the results
    status = False
    while not status:
        status = check_sequence_status(task_id)
        if verbose:
            print("Task {} not done yet....".format(task_id))
        time.sleep(1)
    if verbose:
        print("Task {} finished.".format(task_id))
        print("Retrieving results and generating data frames.")
    return get_sequence_results(task_id)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-f", "--file", help="FASTA file with a single protein sequence"
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbose flag, defaults to false", default=False
    )
    args = parser.parse_args()

    with open(args.file, "r") as f:
        sequence = f.read()

    scan, resolved_scan = run_cath_workflow(sequence, verbose=args.verbose)
    scan.to_csv("scan.csv")
    resolved_scan.to_csv("resolved_scan.csv")
