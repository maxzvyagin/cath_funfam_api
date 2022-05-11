import requests
from pathlib import Path
import os
import stat
import subprocess
from argparse import ArgumentParser

CATH_SUBMISSION_SCRIPT = """
#!/usr/bin/perl
use strict;
use warnings;
use LWP::UserAgent;
 
my $ua = LWP::UserAgent->new;
$ua->timeout(10);
$ua->default_header( 'Accept' => 'application/json' );
 
my $url = 'http://www.cathdb.info/search/by_funfhmmer';
my %data = ();
$data{fasta} = <<'_PARAM';
{}
_PARAM
 
 
 
my $response = $ua->post( $url , \%data );
 
if ( $response->is_success ) {
    print $response->decoded_content;
}
else {
    die $response->status_line;
}
"""


def submit_sequence(seq: str, tmp_dir: Path = Path("/tmp"), verbose=False):
    """Creates a temporary PERL script, and runs it to submit it to the CATH search, returns the task id"""
    filled_script = CATH_SUBMISSION_SCRIPT.format(seq)
    tmp_script = tmp_dir / "cath_submission.pl"
    with open(tmp_script, "w") as f:
        f.write(filled_script)

    # chmod to be executable
    st = os.stat(tmp_script)
    os.chmod(tmp_script, st.st_mode | stat.S_IEXEC)
    # now run the script to submit it, need to parse the output in order to get the task id and return it
    submission_result = subprocess.run([".{}".format(tmp_script)], text=True)
    output = submission_result.stdout
    if verbose:
        print("Task ID: ", output.strip())
    return output.strip()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", help="FASTA file with a single protein sequence")
    args = parser.parse_args()

    with open(args.file, "r") as f:
        sequence = f.read()
        
    task = submit_sequence(sequence, verbose=True)
