package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

sub es_start_index {
    my $self = shift;

    return unless $self->es_has_searchable;

    # reload object to proceed DateTimeColumn etc
    $self->discard_changes;

    warn "Indexing...\n";

    my @fields = $self->es_searchable_fields;
    my %body = map { $_ => $self->{ '_column_data' }{ $_ } } @fields;

    $body{es_id} = $self->es_id(\%body);

    return $self->es_index(\%body);
}

sub es_searchable_fields {

    return shift->result_source->resultset->es_searchable_fields;
}

sub es_has_searchable {

    return shift->result_source->resultset->es_has_searchable;
}

sub es_id {

    return shift->result_source->resultset->es_id(shift);
}

sub es_is_primary {

    return shift->result_source->resultset->es_is_primary;
}

after 'insert' => sub {
    my $self = shift;

    return do {
        if ($self->es_is_primary) {
            warn "Inserting ...";
            $self->es_start_index;
        } else {
            $self;
        }
    };
};

after 'update' => sub {
    my $self = shift;

    return do {
        if ($self->es_is_primary) {
            warn "Updating ...";
            $self->es_start_index;
        } else {
            $self;
        }
    };
};

after 'delete' => sub {
    my $self = shift;

    return do {
        if ($self->es_is_primary) {
            warn "Deleting...\n";
            $self->es_delete;
        } else {
            #$self;
        }
    };
};

sub es_index {

    my ( $self, $body ) = @_;

    my $type = $self->result_source->name;

    $self->es->index(
        index => $self->result_source->schema->es_index_name,
        id    => $body->{es_id},
        type  => $type,
        body  => $body

    );

}

sub es {

    return shift->result_source->schema->es;
}

sub es_delete {

    my ( $self, $entry ) = @_;

    my $type = $self->result_source->name;

    my %columns;

    $self->es->delete(
        id    => $self->es_id( \%columns ),
        type  => $type,
        index => $self->result_source->schema->es_index_name,
    );
}

1;
