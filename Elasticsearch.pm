package DBIx::Class::Elasticsearch;
use strict;
use warnings;

use Search::Elasticsearch;

use YAML::Syck;
use File::Basename;
use Data::Dumper;
use mro 'c3';

my $es;
my $settings;

sub has_searchable {
    my $self = shift;

    return scalar $self->searchable_fields;
}

sub searchable_fields {
    my $self = shift;

    my $klass             = $self->result_class;
    my $cols              = $klass->columns_info;
    my @searchable_fields = grep { $cols->{$_}->{searchable} } keys %{$cols};

    return @searchable_fields;
}

sub es {

    my ($self) = @_;

    my $settings = $self->settings;

    $es = Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ) unless $es;

    return $es;
}

sub settings {
    my $self = shift;
    my $path = dirname(__FILE__);

    if ( !$settings ) {
        my $yml = YAML::Syck::LoadFile("$path/elastic_search.yml");
        die "Could not load settings. elastic_search.yml not found" unless $yml;
        $settings = $yml;
    }

    return $settings;
}

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

sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk = $self->es->bulk_helper;

    for my $row (@$data) {

        my $type = $self->result_source->name;

        my $params = {
            index  => $self->settings->{index},
            id     => sprintf ( "%s_%s", $type, $row->{es_id} ),
            type   => $type,
            source => $row
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

sub es_delete {

    my ( $self, $entry ) = @_;

    my $pk = $self->primary_key;

    my $type = $self->result_source->name;

    $self->es->delete(
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        index => $self->settings->{index},
    );
}

sub post {
    my ( $self, $url, $content ) = @_;

    my $request = HTTP::Request->new( POST => $url );
    $request->content_type('application/json');
    $request->content($content);

    #return $self->user_agent->request($request);
}

sub get {
    my ( $self, $url, $content ) = @_;

    my $request = HTTP::Request->new( GET => $url );
    $request->content_type('application/json');
    $request->content($content);

    #return $self->user_agent->request($request);
}

sub http_delete {
    my ( $self, $url ) = @_;

    my $request = HTTP::Request->new( DELETE => $url );

    #return $self->user_agent->request($request);
}

sub primary_keys {
    my $self = shift;

    my @ids = $self->result_source->primary_columns;
    return @ids;
}

sub es_id {
    my $self = shift;

    my $concat_id = [];

    for my $id ( $self->primary_keys ) {

       push @$concat_id, $self->$id if $self->$id;
    }

    return join '_', @$concat_id;
}

1;
