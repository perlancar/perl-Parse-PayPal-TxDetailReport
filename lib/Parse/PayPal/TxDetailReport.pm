package Parse::PayPal::TxDetailReport;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::Any qw($log);

use Exporter qw(import);
our @EXPORT_OK = qw(parse_paypal_txdetail_report);

use DateTime::Format::Flexible; # XXX find a more lightweight alternative

our %SPEC;

sub _parse_date {
    DateTime::Format::Flexible->parse_datetime(shift)->epoch;
}

$SPEC{parse_paypal_txdetail_report} = {
    v => 1.1,
    summary => 'Parse PayPal transaction detail report into data structure',
    description => <<'_',

The result will be a hashref. The main key is `transactions` which will be an
arrayref of hashrefs.

Dates will be converted into Unix timestamps.

_
    args => {
        files => {
            schema => ['array*', of=>'filename*', min_len=>1],
            description => <<'_',

Files can all be in tab-separated or comma-separated (CSV) format but cannot be
mixed. If there are multiple files, they must be ordered.

_
            pos => 0,
            greedy => 1,
        },
        strings => {
            schema => ['array*', of=>'str*', min_len=>1],
            description => <<'_',

Instead of `files`, you can alternatively provide the file contents in
`strings`.

_
        },
        format => {
            schema => ['str*', in=>[qw/tsv csv/]],
            description => <<'_',

If unspecified, will be deduced from the first filename's extension (/csv/ for
CSV, or /txt|tsv/ for tab-separated).

_
        },
    },
    args_rels => {
        req_one => ['files', 'strings'],
    },
};
sub parse_paypal_txdetail_report {
    my %args = @_;

    my $format = $args{format};

    my @handles;
    my @files;
    if (my $strings = $args{strings}) {
        require IO::Scalar;

        if (!$format) {
            $format = $strings->[0] =~ /\t/ ? 'tsv' : 'csv';
        }
        for my $str (@{ $strings }) {
            my $fh = IO::Scalar->new(\$str);
            push @handles, $fh;
            push @files, "string";
        }
    } elsif (my $files = $args{files}) {
        require File::BOM;

        if (!$format) {
            $format = $files->[0] =~ /\.(csv)\z/i ? 'csv' : 'tsv';
        }
        for my $file (@{ $files }) {
            open my($fh), "<:encoding(utf8):via(File::BOM)", $file
                or return [500, "Can't open file '$file': $!"];

            push @handles, $fh;
            push @files, $file;
        }
    } else {
        return [400, "Please specify files (or strings)"];
    }

    my $res = [200, "OK", {}];

    my $code_parse_row = sub {
        my $row = shift;

        if ($row->[0] eq 'RH') { # row header
            $res->[2]{RH_seen}++ and do {
                $res = [400, "RH row seen twice in a file"];
                goto RETURN_RES;
            };
            $res->[2]{report_generation_date} //= _parse_date($row->[1]);
            $res->[2]{reporting_window} //= $row->[2];
            $res->[2]{account_id} //= $row->[3];
            $res->[2]{report_version} //= $row->[4];
            $row->[4] == 11 or do {
                $res = [400, "Version ($row->[4]) not supported, only version 11 is supported"];
                goto RETURN_RES;
            };
        } elsif ($row->[0] eq 'FH') { # file header
            $res->[2]{FH_seen}++ and do {
                $res = [400, "FH row seen twice in a file"];
                goto RETURN_RES;
            };
            $res->[2]{cur_file_seq} == $row->[1] or do {
                $res = [400, "Unexpected file sequence, expected sequence ".
                            "$res->[2]{cur_file_seq} for file ".
                            "$res->[2]{cur_file}"];
                goto RETURN_RES;
            };
        } elsif ($row->[0] eq 'SH') { # section header
            $res->[2]{SH_seen}++ and do {
                $res = [400, "SH row seen twice in a file"];
                goto RETURN_RES;
            };
        } elsif ($row->[0] eq 'CH') { # column header
            $res->[2]{transaction_columns} //= [@{$row}[1..$#{$row}]];
        } elsif ($row->[0] eq 'SB') { # section body
            my $tx = {};
            my $txcols = $res->[2]{transaction_columns};
            for (1..$#{$row}) {
                my $header = $txcols->[$_-1];
                if ($header =~ /Date$/ && $row->[$_]) {
                    $tx->{$header} = _parse_date($row->[$_]);
                } else {
                    $tx->{$header} = $row->[$_];
                }
            }
            push @{ $res->[2]{transactions} }, $tx;
        } elsif ($row->[0] eq 'SF') { # section footer
            # XXX currently ignored
        } elsif ($row->[0] eq 'FF') { # file footer
            # XXX currently ignored
        } elsif ($row->[0] eq 'RF') { # report footer
            # XXX currently ignored
        } elsif ($row->[0] eq 'SC') { # section count
            # XXX check number of transactions in the section
        } elsif ($row->[0] eq 'RC') { # report count
            unless ($row->[1] == @{ $res->[2]{transactions} }) {
                $res = [400, "Mismatched number of transactions (found=".
                            (scalar @{ $res->[2]{transactions} }).", from RC=".
                            $row->[1]];
                goto RETURN_RES;
            }
        } else {
            $res = [400, "Unknown row type '$row->[0]'"];
            goto RETURN_RES;
        }
    };

    my $code_on_eof = sub {
        delete $res->[2]{cur_file};
        delete $res->[2]{cur_file_seq};
        delete($res->[2]{RH_seen}) or do {
            $res = [400, "No RH row seen"];
            goto RETURN_RES;
        };
        delete($res->[2]{FH_seen}) or do {
            $res = [400, "No RH row seen"];
            goto RETURN_RES;
        };
        delete($res->[2]{SH_seen}) or do {
            $res = [400, "No SH row seen"];
            goto RETURN_RES;
        };
    };

    my $code_on_eor = sub {
        delete $res->[2]{transaction_columns};
    };

    for my $i (0..$#files) {
        $res->[2]{cur_file_seq} = $i+1;
        $res->[2]{cur_file} = $files[$i];
        my $csv;
        if ($format eq 'csv') {
            require Text::CSV;
            $csv = Text::CSV->new
                or return [500, "Cannot use CSV: ".Text::CSV->error_diag];
        }
        if ($format eq 'csv') {
            while (my $row = $csv->getline($handles[$i])) {
                $code_parse_row->($row);
            }
        } else {
            my $fh = $handles[$i];
            while (my $line = <$fh>) {
                chomp($line);
                $code_parse_row->([split /\t/, $line]);
            }
        }
        $code_on_eof->();
    }
    $code_on_eor->();

  RETURN_RES:
    $res;
}

1;
# ABSTRACT:

=head1 SYNOPSIS

 use Parse::PayPal::TxDetailReport qw(parse_paypal_txdetail_report);

 my $res = parse_paypal_txdetail_report(files => []);

Sample result when there is a parse error:

 [400, "Version (10) not supported, only version 11 supported"]

Sample result when parse is successful:

 [200, "OK", {
     account_id => "...",
     report_generation_date => 1467375872,
     report_version         => 11,
     reporting_window       => "A",
     transactions           => [
         {
             "3PL Reference ID"                   => "",
             "Auction Buyer ID"                   => "",
             "Auction Closing Date"               => "",
             "Auction Site"                       => "",
             "Authorization Review Status"        => 1,
             ...
             "Transaction Completion Date"        => 1467273397,
             ...
         },
         ...
     ],
 }]


=head1 DESCRIPTION

This module provides routine to parse PayPal transaction detail report into a
Perl data structure. Version 11 is supported. Multiple files are supported. Both
the tab-separated format and comma-separated (CSV) format are supported.


=head1 SEE ALSO

L<https://www.paypal.com>

Specification of transaction detail report format:
L<https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_Gen_TransactionDetailReport.pdf>
