#!/usr/bin/perl

use DBI;
use CGI qw/:standard/;
use Switch;

my $serverdb = "localhost";
my $dbname = "netflow";
my $dbuser = "flowtools";
my $dbpass = "7ii48aws";
my $forma = new CGI;
my $in = $forma->param("pg");
my $proc_sql_req = $forma->param("proc_sql_req");
my $sql_req_src_ip = $forma->param("sql_req_src_ip");
my $sql_req_dst_ip = $forma->param("sql_req_dst_ip");
my $sql_req_src_port = $forma->param("sql_req_src_port");
my $sql_req_dst_port = $forma->param("sql_req_dst_port");
my $sql_req_date_from = $forma->param("sql_req_date_from");
my $sql_req_date_to = $forma->param("sql_req_date_to");
my $sql_req_time_from = $forma->param("sql_req_time_from");
my $sql_req_time_to = $forma->param("sql_req_time_to");
if ($sql_req_date_from ne '') {
    if ($sql_req_time_from eq '') {
	$sql_req_time_from = "00:00";
    }
    if ($sql_req_date_to eq '') {
	$sql_req_date_to = $sql_req_date_from;
    }
    if ($sql_req_time_to eq '') {
	$sql_req_time_to = "23:59";
    }
}
my $sql_req_proto = $forma->param("sql_req_proto");
my $sql_table = "ln17rb_2024_10";
my $sql_req_limit = 100;
my $use_debug = 1;
#my $sql_tmp_ip = "192.168.37.10";

#header

print qq~
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Netflow File Analyzer Reports</title>
<link href="css/styles.css" rel="stylesheet" media="all" />
</head>
<body>

<div class="sidenav">
  <a href="."><img src="image/logo_120x120.jpg"></a>
  <br>
  <a href="?pg=sql_req">SQL request</a>
  <a href="?pg=top_in">Top IN</a>
  <a href="?pg=top_out">Top OUT</a>
  <a href="?pg=options">Options</a>
</div>

<div class="main">
~;

switch($in) {
    case "sql_req"	{&pg_sql_req}
    case "top_in"	{&pg_top_in}
    case "top_out"	{&pg_top_out}
    case "options"	{&pg_options}
    else	{&pg_main}
}

#footer

print qq~
</div>
</body>
</html>
~;

sub pg_main {

print qq~
  <h1>Sidebar</h1>
  <p>This sidebar is of full height (100%) and always shown.</p>
  <p>Scroll down the page to see the result.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
  <p>Some text to enable scrolling.. Lorem ipsum dolor sit amet, illum definitiones no quo, maluisset concludaturque et eum, altera fabulas ut quo. Atqui causae gloriatur ius te, id agam omnis evertitur eum. Affert laboramus repudiandae nec et. Inciderint efficiantur his ad. Eum no molestiae voluptatibus.</p>
~;
}

sub pg_sql_req {
print qq~
  <h1>SQL REQ</h1>
  <form action=netflow.pl method=post>
  <table class="table_sqlreq">
  <tbody>
  <tr>
    <td>Source IP (src_ip)</td>
    <td><input class="text_ip" name=sql_req_src_ip type="text" placeholder="1-255.0-255.0-255.1-255" title="It should be correct IP address or empty (all)" pattern="([1-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([0-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([0-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([1-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])" value=$sql_req_src_ip></td>
    <td>Destination IP (dst_ip)</td>
    <td><input class="text_ip" name=sql_req_dst_ip type="text" placeholder="1-255.0-255.0-255.1-255" title="It should be correct IP address or empty (all)" pattern="([1-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([0-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([0-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])[.]([1-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5])" value=$sql_req_dst_ip></td>
  </tr>
  <tr>
    <td>Source port (src_port)</td>
    <td><input class="text_port" name=sql_req_src_port type="number" min="0" max="65535" placeholder="0-65535" title="It should be 0, correct port between 1 and 65535, or empty (all)" value=$sql_req_src_port></td>
    <td>Destination port (dst_port)</td>
    <td><input class="text_port" name=sql_req_dst_port type="number" min="0" max="65535" placeholder="0-65535" title="It should be 0, correct port between 1 and 65535, or empty (all)" value=$sql_req_dst_port></td>
  </tr>
  <tr>
    <td>Date from</td>
    <td><input class="date_date" name=sql_req_date_from type="date" value=$sql_req_date_from></td>
    <td>Date to</td>
    <td><input class="date_date" name=sql_req_date_to type="date" value=$sql_req_date_to></td>
  </tr>
  <tr>
    <td>Time from</td>
    <td><input class="time_time" name=sql_req_time_from type="time" value=$sql_req_time_from></td>
    <td>Time to</td>
    <td><input class="time_time" name=sql_req_time_to type="time" value=$sql_req_time_to></td>
  </tr>
  <tr>
    <td>Protocol</td>
    <td><input name=sql_req_proto type=text value=$sql_req_proto></td>
    <td>Traffic summ</td>
    <td>To be here</td>
  </tr>
  <tr>
    <td colspan="4" align="center"><input name="s_button" type="submit"></td>
  </tr>
  </tbody>
  </table>
  <input type=hidden name=proc_sql_req value=start>
  <input type=hidden name=pg value=sql_req>
~;

if ($proc_sql_req eq 'start') {
    &do_sql_req;
}
}

sub do_sql_req {
    $dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection;
    $sql_select = "SELECT INET_NTOA(src_ip),src_port,INET_NTOA(dst_ip),dst_port,proto,packets,bytes,type,utime from $sql_table";
    $w = 0;
    $a = 0;
    if ($sql_req_src_ip ne '') {
	$w = 1;
    }
    if ($sql_req_dst_ip ne '') {
	$w = 1;
    }
    if ($sql_req_src_port ne '') {
	$w = 1;
    }
    if ($sql_req_dst_port ne '') {
	$w = 1;
    }
    if ($sql_req_proto ne '') {
	$w = 1;
    }
    if ($sql_req_date_from ne '') {
	$w = 1;
    }
    if ($w eq 1) {
	$sql_select = $sql_select." where";
    }
    if ($sql_req_src_ip ne '') {
	$sql_select = $sql_select." src_ip=INET_ATON('$sql_req_src_ip')";
	$a = 1;
    }
    if ($sql_req_dst_ip ne '') {
	if ($a eq 1) {
	    $sql_select = $sql_select." and";
	}
	$sql_select = $sql_select." dst_ip=INET_ATON('$sql_req_dst_ip')";
	$a = 1;
    }
    if ($sql_req_src_port ne '') {
	if ($a eq 1) {
	    $sql_select = $sql_select." and";
	}
	$sql_select = $sql_select." src_port=$sql_req_src_port";
	$a = 1;
    }
    if ($sql_req_dst_port ne '') {
	if ($a eq 1) {
	    $sql_select = $sql_select." and";
	}
	$sql_select = $sql_select." dst_port=$sql_req_dst_port";
	$a = 1;
    }
    if ($sql_req_proto ne '') {
	if ($a eq 1) {
	    $sql_select = $sql_select." and";
	}
	$sql_select = $sql_select." proto=$sql_req_proto";
	$a = 1;
    }
    if ($sql_req_date_from ne '') {
	if ($a eq 1) {
	    $sql_select = $sql_select." and";
	}
	$sql_f_df = $sql_req_date_from.$sql_req_time_from."00";
	$sql_f_df =~ s/[-:]//g;
	$sql_f_dt = $sql_req_date_to.$sql_req_time_to."59";
	$sql_f_dt =~ s/[-:]//g;
	$sql_select = $sql_select." utime between $sql_f_df and $sql_f_dt";
	$a = 1;
    }
    $sql_select = $sql_select." limit $sql_req_limit";
    if ($use_debug) {
	print "<p>",$sql_select,"</p>\n"; #debug
    }
    $sth = $dbh->prepare($sql_select);
    $sth->execute ();
    $i = 0;
    while (@row = $sth->fetchrow_array) {
	$i++;
	@sql_src_ip[$i] = @row[0];
	@sql_src_port[$i] = @row[1];
	@sql_dst_ip[$i] = @row[2];
	@sql_dst_port[$i] = @row[3];
	@sql_proto[$i] = @row[4];
	@sql_packets[$i] = @row[5];
	@sql_bytes[$i] = @row[6];
	@sql_type[$i] = @row[7];
	@sql_utime[$i] = @row[8];
    }
    $sth->finish;
    $dbh->disconnect;
    print qq~
  <table class="table_sqlresult">
  <tbody>
  <tr>
    <th>src_ip</th>
    <th>src_port</th>
    <th>dst_ip</th>
    <th>dst_port</th>
    <th>proto</th>
    <th>packets</th>
    <th>bytes</th>
    <th>type</th>
    <th>utime</th>
  </tr>
    ~;
    $i1 = $i;
    $i = 1;
    while ($i1 > 0) {
	print "  <tr>\n";
        print "    <td>@sql_src_ip[$i]</td>\n";
        print "    <td>@sql_src_port[$i]</td>\n";
        print "    <td>@sql_dst_ip[$i]</td>\n";
        print "    <td>@sql_dst_port[$i]</td>\n";
        print "    <td>@sql_proto[$i]</td>\n";
        print "    <td>@sql_packets[$i]</td>\n";
        print "    <td>@sql_bytes[$i]</td>\n";
        print "    <td>@sql_type[$i]</td>\n";
        print "    <td>@sql_utime[$i]</td>\n";
        print "  </tr>\n";
        $i++;
        $i1--;
    }
    print "  </tbody>\n";
    print "  </table>";
}

sub pg_top_in {

print qq~
  <h1>TOP IN</h1>
~;
}
sub pg_top_out {

print qq~
  <h1>TOP OUT</h1>
~;
}
sub pg_options {

print qq~
  <h1>OPTIONS</h1>
~;
}

sub error_connection {

print "Error.\n";
foreach $line_arr(@flowfile_arr_in) {
    open (DUMPFILE, ">>$flowfile_log");
    $line_arr = "$line_arr\n";
    print DUMPFILE $line_arr;
    close (DUMPFILE);
}
die "\n";
}