package DBIx::Class::Elasticsearch::Role::ElasticBlackholeResult;

use strict;
use warnings;

use Moose::Role;

sub es_index {

    my $self = shift;

    my $schema = $self->result_source->schema;
    my $class  = $self->result_source->source_name;

    my $elastic_rs = $schema->dispatcher->{$class};

    for my $rs (@$elastic_rs) {

        eval "use $rs";

        die $@ if $@;

        my $obj = $self->es_obj_builder($rs);

        $self->schema->es_index_obj($obj);
    }
}

sub es_obj_builder {

    my $self = shift;
    my $rs   = shift;

    my $body = { $self->get_columns };
    my $obj = { body => $body };
    $obj->{type} = $rs->type;
    $obj->{body}{es_id} = $self->es_build_id($rs);

    return $obj;

}

sub es_build_id {

    my $self = shift;
    my $rs = shift;

    my $pks = $rs->es_id_columns;

    my $ids = [];

    for my $pk (@$pks) {

        push @$ids, $self->$pk;
    }

    return join '_', @$ids;
}



around 'insert' => sub {
    my $orig = shift;
    my $self = shift;

    warn "Inserting ...\n";
    $self->es_index;
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

    return shift->schema->es;
}

sub schema {

    return shift->result_source->schema;
}

1;
