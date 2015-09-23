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
    my %columns = $self->get_columns;
    my %body;
    for my $field ( @fields ) {

        $body{$field} = $columns{$field} unless $columns{$field} eq '0000-00-00';
    }

    use DDP;
    p %body;

    return $self->es_index(\%body);
}

sub es_searchable_fields {

    return shift->result_source->resultset->es_searchable_fields;
}

sub es_has_searchable {

    return shift->result_source->resultset->es_has_searchable;
}

after 'insert' => sub {
    my $self = shift;

    return do {
        if ($self->es_has_searchable) {
            $self->es_start_index;
        } else {
            $self;
        }
    }
};

after 'update' => sub {
    my $self = shift;

    return do {
        if ($self->es_has_searchable) {
            $self->es_start_index;
        } else {
            $self;
        }
    }
};

after 'delete' => sub {
    my $self = shift;

    return do {
        if ($self->es_has_searchable) {
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
        index => $self->result_source->schema->es_index_name,
        id    => sprintf( "%s_%s", $type, $self->es_id ),
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

    $self->es->delete(
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        index => $self->result_source->schema->es_index_name,
    );
}

sub es_id {
    my $self = shift;

    my @ids = $self->id;

    return join '_', @ids;
}

1;
