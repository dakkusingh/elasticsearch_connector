<?php

namespace Drupal\elasticsearch_connector\ElasticSearch\Parameters\Factory;

use Drupal\search_api\Item\FieldInterface;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use Drupal\elasticsearch_connector\Event\PrepareMappingEvent;

/**
 * Class MappingFactory.
 */
class MappingFactory {

  /**
   * Helper function. Get the elasticsearch mapping for a field.
   *
   * @param FieldInterface $field
   *
   * @return array|null
   */
  public static function mappingFromField(FieldInterface $field) {
    try {
      $type = $field->getType();
      $mappingConfig = NULL;

      switch ($type) {
        case 'text':
          $mappingConfig = [
            'type' => 'text',
            'boost' => $field->getBoost(),
            'fields' => [
              "keyword" => [
                "type" => 'keyword',
                'ignore_above' => 256,
              ]
            ]
          ];

        case 'uri':
        case 'string':
        case 'token':
          $mappingConfig = [
            'type' => 'keyword',
          ];

        case 'integer':
        case 'duration':
          $mappingConfig = [
            'type' => 'integer',
          ];

        case 'boolean':
          $mappingConfig = [
            'type' => 'boolean',
          ];

        case 'decimal':
          $mappingConfig = [
            'type' => 'float',
          ];

        case 'date':
          $mappingConfig = [
            'type' => 'date',
            'format' => 'epoch_second',
          ];

        case 'attachment':
          $mappingConfig = [
            'type' => 'attachment',
          ];
      }
    }
    catch (ElasticsearchException $e) {
      watchdog_exception('Elasticsearch Backend', $e);
    }

    // Allow other modules to alter mapping config before we create it.
    $dispatcher = \Drupal::service('event_dispatcher');
    $prepareMappingEvent = new PrepareMappingEvent($mappingConfig, $type, $field);
    $event = $dispatcher->dispatch(PrepareMappingEvent::PREPARE_MAPPING, $prepareMappingEvent);
    $mappingConfig = $event->getMappingConfig();

    return $mappingConfig;
  }

}
