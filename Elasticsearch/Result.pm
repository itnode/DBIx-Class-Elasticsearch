package DBIx::Class::Elasticsearch::Result;
use strict;
use warnings;
use JSON;
use Data::Dumper;

use Moose;

extends 'DBIx::Class::Elasticsearch';

sub index {
    my $self = shift;

    return unless $self->has_searchable;

    warn "Indexing...\n";

    my @fields = $self->searchable_fields;
    my %body = map { $_ => $self->{ '_column_data' }{ $_ } } @fields;

    return $self->es_index(\%body);
}

after 'insert' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            $self->index;
        } else {
            $self;
        }
    }
}

after 'update' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            $self->index;
        } else {
            $self;
        }
    }
}

after 'delete' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            warn "Deleting...\n";
            $self->es_delete;
        } else {
            #$self;
        }
    }
}

sub build_json {
    my $self = shift;
    my $pk = $self->primary_key;

    my @json = (encode_json({ index => { '_id' => $self->$pk } }));
    push(@json, encode_json($self->{ '_column_data' }));

    return @json;
}

1;
