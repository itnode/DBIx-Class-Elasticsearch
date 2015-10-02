package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

sub es_index {

    my $self = shift;

    my $schema = $self->result_source->schema;
    my $class  = $self->result_source->source_name;

    my $elastic_rs  = $schema->dispatcher->{$class};
    my $dbix_rs     = $self->result_source->resultset;
    my $me          = $dbix_rs->current_source_alias;
    my $dbix_params = { map { $me . "." . $_ => $self->$_ } $self->primary_columns };

    $dbix_rs = $self->result_source->resultset->search_rs($dbix_params);

    for my $rs (@$elastic_rs) {

        eval "use $rs";

        warn $@ if $@;

        if ( $rs->es_is_primary ) {

            $rs->es_index($dbix_rs);

        } elsif ( $rs->es_is_nested ) {

            my $higher_rs = $dbix_rs;
            my $higher_class = $higher_rs->result_source->source_name

            while ( !$rs->es_is_primary($higher_class) ) {

                $higher_class = $higher_rs->result_source->source_name;
                my $rel = $rs->relation_dispatcher->{nested}{$higher_class};
                $higher_rs = $higher_rs->search_related( $rel, {} );
            }

            $rs->es_index->($dbix_rs);
        }
    }
}

sub es_delete {

    my ($self) = @_;

    my $schema = $self->result_source->schema;
    my $class  = $self->result_source->source_name;

    my $elastic_rs  = $schema->dispatcher->{$class};
    my $dbix_rs     = $self->result_source->resultset;
    my $me          = $dbix_rs->current_source_alias;
    my $dbix_params = { map { $me . "." . $_ => $self->$_ } $self->primary_columns };

    $dbix_rs = $self->result_source->resultset->search_rs($dbix_params);

    for my $rs (@$elastic_rs) {

        eval "use $rs";

        warn $@ if $@;

        $rs->es_delete($dbix_rs);
    }
}

after 'insert' => sub {
    my $self = shift;

    warn "Inserting ...\n";
    $self->es_index;
};

after 'update' => sub {
    my $self = shift;

    warn "Updating ...\n";
    $self->es_index;
};

before 'delete' => sub {
    my $self = shift;

    warn "Deleting...\n";
    $self->es_delete;
};

sub es_index_transfer {

    my ( $self, $body ) = @_;

    my $type = $self->result_source->name;

    my $parent = {};
    if ( $self->es_is_child ) {

        $parent = { parent => $self->es_parent };
    }

}

sub es {

    return shift->result_source->schema->es;
}

1;
