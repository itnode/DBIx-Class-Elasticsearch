package DBIx::Class::Elasticsearch::Result;
use strict;
use warnings;
use JSON;
use Data::Dumper;
use base qw(DBIx::Class::Elasticsearch);

sub url {
    my $self = shift;

    my $pk = $self->primary_key;
    my $url = $self->next::method(@_);

    return $url . $pk;
}

sub index {
    my $self = shift;

    return unless $self->has_searchable;

    warn "Indexing...\n";

    my @fields = $self->searchable_fields;
    my %body = map { $_ => $self->{ '_column_data' }{ $_ } } @fields;

    return $self->es_index(\%body);
}

sub insert {
    my $self = shift;

    $self->next::method(@_);

    return do {
        if ($self->has_searchable) {
            $self->index;
        } else {
            $self;
        }
    }
}

sub update {
    my $self = shift;

    $self->next::method(@_);

    return do {
        if ($self->has_searchable) {
            $self->index;
        } else {
            $self;
        }
    }
}

sub delete {
    my $self = shift;

    $self->next::method(@_);

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
