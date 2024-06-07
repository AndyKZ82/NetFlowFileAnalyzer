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
my $sql_table = "test_2024_06";
my $sql_req_limit = 100;
my $sql_tmp_ip = "192.168.37.10";

#header

print qq~
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {
    font-family: "Lato", sans-serif;
}

.sidenav {
    height: 100%;
    width: 200px;
    position: fixed;
    z-index: 1;
    top: 0;
    left: 0;
    background-color: #000;
    overflow-x: hidden;
    padding-top: 20px;
}

.sidenav a {
    padding: 6px 8px 6px 16px;
    text-decoration: none;
    font-size: 25px;
    color: #818181;
    display: block;
}

.sidenav a:hover {
    color: #f1f1f1;
}

.main {
    margin-left: 200px; /* Same as the width of the sidenav */
    font-size: 28px; /* Increased text to enable scrolling */
    padding: 0px 10px;
}

@media screen and (max-height: 450px) {
    .sidenav {padding-top: 15px;}
    .sidenav a {font-size: 18px;}
}
</style>
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
  <h2>Sidebar</h2>
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
  <h2>SQL REQ</h2>
  <form action=netflow.pl method=post>
  <table border="2">
  <tbody>
  <tr>
    <td>Source IP (src_ip)</td>
    <td><input name=sql_req_src_ip type=text value=$sql_req_src_ip></td>
    <td>Destination IP (dst_ip)</td>
    <td>Input form</td>
  </tr>
  <tr>
    <td>Source port (src_port)</td>
    <td>Input form</td>
    <td>Destination port (dst_port)</td>
    <td>Input form</td>
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
    # where src_ip=INET_ATON(?) limit $sql_req_limit";
    if ($sql_req_src_ip ne '') {
	$sql_select=$sql_select." where src_ip=INET_ATON('$sql_req_src_ip')";
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
      <table border=1>
      <tbody>
      <tr>
        <td>src_ip</td>
        <td>src_port</td>
        <td>dst_ip</td>
        <td>dst_port</td>
        <td>proto</td>
        <td>packets</td>
        <td>bytes</td>
        <td>type</td>
        <td>utime</td>
      </tr>
    ~;
    $i1 = $i;
    $i = 1;
    while ($i1 > 0) {
	print "<tr>\n";
        print "<td>@sql_src_ip[$i]</td>\n";
        print "<td>@sql_src_port[$i]</td>\n";
        print "<td>@sql_dst_ip[$i]</td>\n";
        print "<td>@sql_dst_port[$i]</td>\n";
        print "<td>@sql_proto[$i]</td>\n";
        print "<td>@sql_bytes[$i]</td>\n";
        print "<td>@sql_packets[$i]</td>\n";
        print "<td>@sql_type[$i]</td>\n";
        print "<td>@sql_utime[$i]</td>\n";
        print "</tr>\n";
        $i++;
        $i1--;
    }
    print "</tbody>\n";
    print "</table>\n";
}

sub pg_top_in {

print qq~
  <h2>TOP IN</h2>
~;
}
sub pg_top_out {

print qq~
  <h2>TOP OUT</h2>
~;
}
sub pg_options {

print qq~
  <h2>OPTIONS</h2>
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