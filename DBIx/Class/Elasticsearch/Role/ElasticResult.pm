package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

sub es_index {
    my $self = shift;

    return unless $self->es_has_searchable;

    # reload object to proceed DateTimeColumn etc
    $self->discard_changes;

    warn "Indexing...\n";

    my $rs       = $self->result_source->resultset;
    my $prefetch = $self->result_source->resultset->es_build_prefetch;

    my %columns = $self->get_columns;

    my $me = $rs->current_source_alias;

    my $query = { map { sprintf( '%s.%s', $me, $_ ) => $columns{$_} } $self->result_source->primary_columns };

    $rs = $rs->search_rs( $query, $prefetch );
    $rs->result_class('DBIx::Class::ResultClass::HashRefInflator');

    my $body = [ $rs->all ];

    return $self->es_index( $body->[0] );
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
        if ( $self->es_is_primary ) {
            warn "Inserting ...";
            $self->es_index;
        } else {
            $self;
        }
    };
};

after 'update' => sub {
    my $self = shift;

    return do {
        if ( $self->es_is_primary ) {
            warn "Updating ...";
            $self->es_index;
        } else {
            $self;
        }
    };
};

after 'delete' => sub {
    my $self = shift;

    return do {
        if ( $self->es_is_primary ) {
            warn "Deleting...\n";
            $self->es_delete;
        } else {

            #$self;
        }
    };
};

sub es_index_transfer {

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
