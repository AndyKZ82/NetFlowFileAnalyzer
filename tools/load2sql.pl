#!/usr/local/bin/perl -w

use DBI;
use Time::localtime;
#use POSIX ":sys_wait_h";

#########################
#########################
my $serverdb = "localhost";
my $dbname = "netflow";
my $dbuser = "flowtools";
my $dbpass = "7ii48aws";
my $ipacct_log = "/tmp/load2sql.log";
my @ipacct_arr;
my @ipacct_arr_in;

my $fcat = "/usr/local/bin/flow-cat";
my $fprint = "/usr/local/bin/flow-print";
my $host = "m";
my $interface = "g";
##########################
##########################

$gm = localtime();
$year = ($gm->year()) + 1900;
$month = ($gm->mon()) + 1;
if ($month < "10") {
    $month = "0"."$month";
}
$mday = $gm->mday();
#$date = "$mday-$mounth-$year"; #why?
$hour = $gm->hour();
$min = $gm->min();
$sec = $gm->sec();
$hour=sprintf("%02d",$hour);
$min=sprintf("%02d",$min);
$sec=sprintf("%02d",$sec);
$time = "$hour\:$min\:$sec"; #current time - 15:05:40
$table_date = "$year\_$month";

my $date_ins;
my $time_ins;

my $mday2;
my $flowpath;
my $flowfile;
my @flowhour;
my $min2;
my $hour2;

my $year3;
my $month3;
my $mday3;
my $hour3;
my $minutes3;
my $seconds3;

if ($mday < "10") {
        $mday2 = "0"."$mday";
        }
        else {
        $mday2 = $mday;
}

$flowpath = "/var/flow/test/$year-$month/$year-$month-$mday2";

$min2 = $min-1;

if ($hour == "0") {
        $hour2 = "23";
        }
        else {
        $hour2 = $hour-1;
}

if ($hour2 < "10") {
        $hour2 = "0"."$hour2";
        }

$flowfile = "ft-v05."."*";

@flows = `ls $flowpath/$flowfile`;

while (@flows) {

        $frow = shift @flows;

        chomp($frow);

        system "$fcat $frow \| $fprint \| grep -v 'prot' > $ipacct_log";

        my ($part01, $part02) = split /\+/, $frow, 2;
        my ($part11, $part12, $part13) = split /\./, $part01, 3;
        ($year3, $month3, $mday3) = split /\-/, $part12, 3;
        ($hour3, $minutes3, $seconds3) = split /(?(?{ pos() % 2 })(?!))/, $part13, 3;

        $date_ins = "$mday3-$month3-$year3";
        $time_ins = "$hour3:$minutes3";

        &parse_log_file;
        &check_in_mysql;
        &insert_data_db;
}

sub check_in_mysql {

my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
                or &error_connection;
$sth = $dbh->prepare("SHOW tables");
$sth->execute ();
my @row;
my $tables;
while (@row = $sth->fetchrow_array) {
        foreach $tables (@row){
                push @dbtables, $tables;
        }
}
$crt_tbl="yes";
while (@dbtables) {
        $table = shift @dbtables;
        if (defined $table) {
                if ($table eq $table_date) {
                        $crt_tbl="no";
                }
        }
}

if ($crt_tbl eq "yes") {
        print "Create Table\n";
        &crt_table_log;
}

$sth->finish;
$dbh->disconnect;

}

sub error_connection {

print "Error.\n";
foreach $line_arr(@ipacct_arr_in) {

        open (DUMPFILE, ">>$ipacct_log");
        $line_arr = "$line_arr\n";
        print DUMPFILE $line_arr;
        close (DUMPFILE);
}
die "\n";

}

sub crt_table_log {

        my ($dbh,$sth,$count);
        $dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
                or &error_connection;
        $select = "CREATE  TABLE $table_date (src_ip INT UNSIGNED NOT NULL,src_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL ,dst_ip INT UNSIGNED NOT NULL,dst_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL, proto TINYINT UNSIGNED,
packets int(8), bytes bigint(20) default 0,date_ins varchar(32), time_ins time,host  varchar(128), interface varchar(8),index (src_ip),index
(dst_ip),index (proto),index (packets), index (bytes),index (host), index (time_ins), index (date_ins), index (interface)) ENGINE = MyISAM";
        $sth = $dbh->prepare("$select");
        $sth->execute ();
        $sth->finish;
        $dbh->disconnect;

}

sub insert_data_db {
my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
                or &error_connection_in;
$insert = "INSERT INTO $table_date (src_ip,src_port,dst_ip,dst_port,proto,packets,bytes,date_ins,time_ins,host,interface) VALUES (INET_ATON(?),?,INET_ATON(?),?,?,?,?,?,?,?,?)";

$sth = $dbh->prepare("$insert");
#print "$insert\n";
while (@ipacct_arr_in) {
        $line_in = shift @ipacct_arr_in;
#       ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$date_ins,$time_ins,$host,$interface)=split(/[\s\t]+/,$line_in);
        ($src_ip,$dst_ip,$proto,$src_port,$dst_port,$bytes,$packets)=split(/[\s\t]+/,$line_in);
        if (!defined $proto){
                $proto="0";
        }
        if (!defined $packets){
                $packets="0";
        }
        if (!defined $bytes){
                $bytes="0";
        }
#        if ($dst_ip =~ m/.255/) {next;}
#        if ($dst_ip =~ m/239./) {next;}
#        if ($dst_ip =~ m/224./) {next;}
#        if ($proto eq "17" and $src_port eq "53") {next;}
#        if ($proto eq "17" and $dst_port eq "53") {next;}
#        if ($proto eq "1") {next;}
#        if ($src_ip eq "10.2.214.20") {next;}
#        if ($dst_ip eq "10.2.214.20") {next;}
#        if ($src_ip eq "81.30.196.90") {next;}
#        if ($ip_to eq "81.30.196.90") {next;}
        $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$date_ins,$time_ins,$host,$interface);
}

$sth->finish;
$dbh->disconnect;
}

sub parse_log_file {
open (PARSFILE, "$ipacct_log");
while ($line_parse=<PARSFILE>) {
        chomp $line_parse;
        $line_parse =~ s/[\s\t]+/\t/g;
        push @ipacct_arr_in, $line_parse;
}
close (PARSFILE);
truncate ("$ipacct_log",0);
}

exit(0);
