#!/usr/bin/env perl

use strict;
use Mojolicious::Lite -signatures;

plugin Database => {
	dsn => 'dbi:mysql:dbname=gaz',
	username => 'worker',
	password => '',
	no_disconnect => 1,
	dbi_attr => { 'AutoCommit' => 1, 'RaiseError' => 1, 'PrintError' =>1 },
	on_connect => sub {
		my $dbh = shift or die $DBI::errstr;
	}
};

my %flags = (
	'<=' => 4, # Указаны веса для сортировки по хронологии
	'**' => 3,
	'==' => 3,
	'=>' => 2,
	'->' => 1,
);

sub send_db {
	my ( $dbh, $statement, %values ) = @_;

	my %queries = (
		message => 'INSERT INTO message (int_id, created, str, id) VALUES (?, ?, ?, ?)',
		log     => 'INSERT INTO log (int_id, created, str, address) VALUES (?, ?, ?, ?)',
	);

	my $sth = $dbh->prepare($queries{$statement});
	$sth->execute( map { $values{$_} } (
			qw/ int_id created str /,
			$statement eq 'message' ? 'id' : $statement eq 'log' ? 'address' : ''
	) );
}

sub check_datetime {
	my ( $dbh, $value ) = @_;
	
	return $dbh->selectall_array( "SELECT TO_SECONDS(?) IS NOT NULL AS valid", undef, $value );
}

get '/load' => sub ($c) {
	my ( $message_count, $log_count, $total_count ) = ( 0, 0, 0 );

	# Очистка данных от предыдущей загрузки
	$c->db->do("DELETE FROM message");
	$c->db->do("DELETE FROM log");

	open my $fh, '<', 'out';
	while (my $row = <$fh>) {
		chomp $row;
		$total_count++;

		my ( %values, $possible_flag, $rest_row );
		# Первые поля с постоянным размером - выделяются по размеру полей
		# Поле флага не всегда заполнено флагом - учитывается это
		( $values{created}, $values{int_id}, $possible_flag, $rest_row ) = unpack 'A19xA16xA2A*', $row;

		# Проверка, что БД понимает выделенное из строки время
		unless ( check_datetime $c->db, $values{created} ) {
			app->log->error( sprintf "Not valid date: %s", $row );
			next;
		} else {
			$values{str} = join ' ', $values{int_id}, ( $possible_flag . $rest_row );
		}

		# Проверка на обязательное поле int_id
		# 2012-02-13 15:00:55 SMTP connection from [109.70.26.4] (TCP/IP connection count = 1)
		unless ( $values{int_id} =~ /^\w{6}-\w{6}-\w\w$/ ) {
			app->log->warn( sprintf "Not valid int_id: %s", $row );
			next;
		}

		# Остальные поля выделяются по предположения, что поля разделены пробелом и всегда начинаются
		# с буквы и знака равно. Например " H=".
		my ( $possible_address, %data ) = split /\s(\w=)/, $rest_row;

		# В поле S= есть id, который выделяется по ТЗ
		( undef, $values{id} ) = split 'id=', $data{'S='} if $data{'S='};
		if ( $values{id} && $flags{$possible_flag} && $flags{$possible_flag} == 4 ) {
			# 2012-02-13 14:39:22 1RwtJa-0009RI-KL <= tpxmuwr@somehost.ru H=mail.somehost.com [84.154.134.45] P=esmtp S=1716 id=120213143628.BLOCKED.453962@whois.somehost.ru
			send_db( $c->db, 'message', %values );

			$message_count++;
		} else {
			# Указан флаг
			if ( $flags{$possible_flag} ) {
				# 2012-02-13 15:00:55 1RwteR-000Om4-65 == psqgg@yandex.ru R=dnslookup T=remote_smtp defer (-1): domain matches queue_smtp_domains, or -odqs set
				# " psqgg@yandex.ru" - убирается лидирующий пробел
				( $values{address} ) = unpack 'xA*', $possible_address;
			}

			send_db( $c->db, 'log', %values );

			$log_count++;
		}
	}
	close $fh;

	$c->render(template => 'load', messages => $message_count, logs => $log_count, total => $total_count );
};

get '/search' => sub ($c) {
	my $query = $c->param('query') || '';

	# Выборка int_id с указанным получателем с лимитом больше 100 для вывода на форме, что данных больше
	my @int_ids = $c->db->selectall_array(
		"SELECT DISTINCT int_id FROM log WHERE address LIKE ? ORDER BY int_id, created LIMIT 101",
		undef,
		sprintf( '%%%s%%', $query ),
	);
	# Выборка записей с найденными int_id с лимитом 150 что бы уменьшить разорванность хронологии,
	# которая ниже собирается по флагам (время слишком дискретно для этого)
	my $sql = sprintf(
		"
			SELECT created, str, int_id FROM log WHERE int_id IN (%s)
			UNION ALL SELECT created, str, int_id FROM message WHERE int_id IN (%s)
			ORDER BY int_id LIMIT 150
		",
		( "'" . join( "','", map { $_->[0] } @int_ids ) . "'" ) x 2,
	);
	my @rows = $c->db->selectall_array($sql);
	my $is_exceeded = @rows > 100 ? 1 : 0;

	# Если в одну секунду проходит вся цепочка обработки почты, то сортировка по флагу для удобного
	# восприятия хронологии - поступление, попытки отправки, отправка, завершающее сообщение.
	# Так как структура БД описана в ТЗ и не предусматривает колонку флага, то собирается "на лету".
	# Добавляется возможный флаг к выборке
	push @$_, unpack 'x17A2', $_->[1] for @rows;
	@rows = sort {
		$a->[2] cmp $b->[2]                                               # int_id
		|| $a->[0] cmp $b->[0]                                            # created
		|| ( $flags{ $b->[3] } || 0 ) <=> ( $flags{ $a->[3] } || 0 )      # flag
	} @rows;

	@rows = @rows[ 0 .. 99 ];
	$c->render( template => 'search', rows => \@rows, query => $query, is_exceeded => $is_exceeded );
};

get '/' => sub ($c) {
	$c->render(template => 'index');
};

app->start;
__DATA__

@@ load.html.ep
% layout 'default';
<span>Loaded messages - <%= $messages %>, logs - <%= $logs %>, total - <%= $total %></span>

@@ search.html.ep
% layout 'default';
<form action="">
<input name=query type=text placeholder="address" value="<%= $query %>">
<input type="submit">
</form>
% if ( ref $rows eq 'ARRAY' ) {
	% if ( $is_exceeded ) {
	<br><b>More than 100</b>
	% }
	<div>
		% for my $row ( @$rows ) {
		<p><%= $row->[0] %> <%= $row->[1] %></p>
		% }
	</div>
% }

@@ index.html.ep
% layout 'default';
<a href="/load">Load data</a>
<br>
<a href="/search">Search by address</a>

@@ layouts/default.html.ep
<!DOCTYPE html>
<html>
  <head><title><%= title %></title></head>
  <body><%= content %></body>
</html>
