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

sub searchable_fields {

    return shift->result_source->resultset->searchable_fields;
}

sub has_searchable {

    return shift->result_source->resultset->has_searchable;
}

after 'insert' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            $self->es_index;
        } else {
            $self;
        }
    }
};

after 'update' => sub {
    my $self = shift;

    return do {
        if ($self->has_searchable) {
            $self->es_index;
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

sub es_index {

    my ( $self, $body ) = @_;

    my $type = $self->result_source->name;

    $self->es->index(
        index => $self->settings->{index},
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        body  => $body

    );

}

sub es_delete {

    my ( $self, $entry ) = @_;

    my $type = $self->result_source->name;

    $self->es->delete(
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        index => $self->settings->{index},
    );
}

sub es_id {
    my $self = shift;

    my $concat_id = [];

    my @ids = $row->id;

    return join '_', @ids;
}

1;
