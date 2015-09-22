package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

with 'DBIx::Class::Elasticsearch::Role::ElasticBase';

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
};

after 'update' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            $self->index;
        } else {
            $self;
        }
    }
};

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
};


1;
