use strict;
use warnings;
use Test::More;
use Politequeue;

# NOTE: This test is for new features and perl implementation specifics that are
# not applicable to the original python project

subtest 'test_new_constructor_formats' => sub {
    # Single argument: filename
    my $q1 = Politequeue->new(':memory:');
    isa_ok($q1, 'Politequeue', 'Single arg constructor returns Politequeue');
    $q1->put('a');
    is($q1->qsize, 1, 'qsize works for single arg');
    is($q1->pop->{data}, 'a', 'pop works for single arg');

    # new constructor keys
    my $q2 = Politequeue->new( db => ':memory:');
    isa_ok($q2, 'Politequeue', 'db arg maps to filename_or_conn');
};

done_testing;