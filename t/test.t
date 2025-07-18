use strict;
use warnings;
use Test::More;
use FindBin;
use Politequeue;
use Time::HiRes qw(sleep);
use constant {
    MESSAGE_STATUS_READY => 0,
    MESSAGE_STATUS_LOCKED => 1,
    MESSAGE_STATUS_DONE => 2,
    MESSAGE_STATUS_FAILED => 3,
};

sub single_queue {
    my ($queue_name) = @_;
    my %args = (memory => 1);
    $args{queue_name} = $queue_name if defined $queue_name;
    return Politequeue->new(%args);
}

sub queue_with_data {
    my ($q) = @_;
    $q->put("hello");
    $q->put("world");
    $q->put("foo");
    $q->put("bar");
    return $q;
}

subtest 'test_isolation_level' => sub {
    my $q_mem = Politequeue->new(memory => 1);
    ok($q_mem->{conn}->{AutoCommit}, 'AutoCommit is on for memory');
    my $q_file = Politequeue->new(filename_or_conn => ':memory:');
    ok($q_file->{conn}->{AutoCommit}, 'AutoCommit is on for file :memory:');
    my $dbh = DBI->connect("dbi:SQLite:dbname=:memory:", "", "", { RaiseError => 1, AutoCommit => 0 });
    my $q_dbh = Politequeue->new(filename_or_conn => $dbh);
    ok($q_dbh->{conn}->{AutoCommit}, 'AutoCommit is forced on for existing dbh');
};

subtest 'test_insert_pop' => sub {
    my $q = single_queue();
    my $first = $q->put("hello");
    $q->put("world");
    $q->put("foo");
    $q->put("bar");

    my $task = $q->pop();
    is($task->{data}, "hello", "Popped data is correct");
    is($task->{message_id}, $first->{message_id}, "Popped message_id is correct");
    is($task->{status}, MESSAGE_STATUS_LOCKED, "Popped status is LOCKED");
    is($task->{done_time}, undef, "Popped done_time is null");
};

subtest 'test_get_unknown' => sub {
    my $q = single_queue();
    is($q->get("nothing"), undef, "Getting unknown message_id returns undef");
};

subtest 'test_pop_all_locked' => sub {
    my $q = queue_with_data(single_queue());
    for (1..4) { $q->pop() }
    is($q->pop(), undef, "Popping from a fully locked queue returns undef");
};

subtest 'test_basic_actions' => sub {
    my $q = queue_with_data(single_queue());
    my $task = $q->pop();
    is($task->{data}, "hello", "Popped data is correct");

    is($q->peek()->{data}, "world", "Peeked data is correct");
    is($q->peek()->{status}, MESSAGE_STATUS_READY, "Peeked status is READY");

    $q->done($task->{message_id});

    my $already_done = $q->get($task->{message_id});
    is($already_done->{status}, MESSAGE_STATUS_DONE, "Status is DONE after marking done");

    ok($already_done->{done_time} >= $already_done->{lock_time}, "done_time >= lock_time");
    ok($already_done->{lock_time} >= $already_done->{in_time}, "lock_time >= in_time");
};

subtest 'test_queue_size' => sub {
    my $q = queue_with_data(single_queue());
    is($q->qsize(), 4, "Initial qsize is 4");
    my $task = $q->pop();
    $q->put("x");
    $q->put("y");
    is($q->qsize(), 6, "qsize is 6 after pop and puts");
    $q->done($task->{message_id});
    is($q->qsize(), 5, "qsize is 5 after done");
};

subtest 'test_prune' => sub {
    my $q = queue_with_data(single_queue());
    while (!$q->empty()) {
        my $t = $q->pop();
        $q->done($t->{message_id});
    }
    $q->prune();
    my $count = $q->{conn}->selectrow_array("SELECT COUNT(*) FROM $q->{table} WHERE status = ?", undef, MESSAGE_STATUS_DONE);
    is($count, 0, "Prune removes DONE messages");
};

subtest 'test_max_size' => sub {
    my $q = Politequeue->new(memory => 1, maxsize => 5);
    for my $i (1..5) {
        $q->put("data_$i");
    }
    is($q->qsize(), 5, "Queue is at max size");
    ok($q->full(), "Queue is full");

    eval { $q->put("new") };
    like($@, qr/Max queue length reached/, "Putting to a full queue throws error");

    $q->pop();
    ok(!$q->full(), "Queue is not full after pop");
};

subtest 'test_empty' => sub {
    my $q_with_data = queue_with_data(single_queue());
    ok(!$q_with_data->empty(), "Queue with data is not empty");

    my $q2 = single_queue();
    ok($q2->empty(), "New queue is empty");
};

subtest 'test_list_locked' => sub {
    my $q = single_queue();
    $q->put("foo");
    my $task = $q->pop();
    sleep(0.2);
    is(scalar(@{$q->list_locked(0.1)}), 1, "Finds 1 locked task with 0.1s threshold");
    is(scalar(@{$q->list_locked(20)}), 0, "Finds 0 locked tasks with 20s threshold");
    $q->done($task->{message_id});
    is(scalar(@{$q->list_locked(0.1)}), 0, "Finds 0 locked tasks after done");
};

subtest 'test_retry_failed' => sub {
    my $q = single_queue();
    $q->put("foo");
    my $task = $q->pop();
    $q->mark_failed($task->{message_id});
    is($q->get($task->{message_id})->{status}, MESSAGE_STATUS_FAILED, "Status is FAILED");
    $q->retry($task->{message_id});
    my $retried_task = $q->get($task->{message_id});
    is($retried_task->{status}, MESSAGE_STATUS_READY, "Status is READY after retry");
    is($retried_task->{done_time}, undef, "done_time is null after retry");
    is($q->qsize(), 1, "qsize is 1 after retry");
};

subtest 'test_count_failed' => sub {
    my $q = single_queue();
    $q->put("foot");
    my $task = $q->pop();
    $q->mark_failed($task->{message_id});
    is(scalar(@{$q->list_failed()}), 1, "Finds 1 failed task");
};

subtest 'test_multiple_queues' => sub {
    my $q1 = Politequeue->new(filename_or_conn => ':memory:', queue_name => 'q1');
    my $q2 = Politequeue->new(filename_or_conn => $q1->{conn}, queue_name => 'q2');

    $q1->put("a");
    $q1->put("b");
    is($q1->qsize(), 2, "Q1 size is 2");
    is($q2->qsize(), 0, "Q2 size is 0");

    $q2->put("c");
    $q2->put("d");
    is($q2->qsize(), 2, "Q2 size is 2");

    is($q1->pop()->{data}, 'a', "Pop from q1");
    is($q1->peek()->{data}, 'b', "Peek from q1");
    is($q2->peek()->{data}, 'c', "Peek from q2");
    is($q2->pop()->{data}, 'c', "Pop from q2");
};

done_testing();
