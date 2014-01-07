<?php
/**
 * @file
 * Created on Jan 06, 2014
 *
 *
 * TODO: Fix the comments of the functions!
 *
 */

function elasticsearch_watchdog_settings() {
  $form = array();

  $clusters = elasticsearch_connector_cluster_load_all(TRUE, TRUE);
  $form['#old_cluster_id'] = variable_get('elasticsearch_watchdog_cluster_id', '');

  $form['elasticsearch_watchdog_cluster_id'] = array(
    '#type' => 'select',
    '#title' => t('Select cluster'),
    '#required' => TRUE,
    '#default_value' => variable_get('elasticsearch_watchdog_cluster_id', ''),
    '#options' => $clusters,
    '#description' => t('Select the cluster you want to hold the logs.')
  );

  $form['elasticsearch_watchdog_num_of_shards'] = array(
    '#type' => 'textfield',
    '#title' => t('Number of shards'),
    '#required' => TRUE,
    '#default_value' => variable_get('elasticsearch_watchdog_num_of_shards', ELASTICSEARCH_WATCHDOG_DEFAULT_SHARDS),
    '#description' => t('Enter the number of shards for the index.')
  );

  $form['elasticsearch_watchdog_num_of_replica'] = array(
    '#type' => 'textfield',
    '#title' => t('Number of replica'),
    '#default_value' => variable_get('elasticsearch_watchdog_num_of_replica', ELASTICSEARCH_WATCHDOG_DEFAULT_REPLICA),
    '#description' => t('Enter the number of shards replicas.')
  );

  $form['#validate'] = array('elasticsearch_watchdog_settings_validate');
  $form['#submit'] = array('elasticsearch_watchdog_settings_submit');

  return system_settings_form($form);
}

/**
 * Validate the setting form submission.
 *
 * @param array $form
 * @param array $form_state
 */
function elasticsearch_watchdog_settings_validate($form, &$form_state) {
  $values = $form_state['values'];
  $client = elasticsearch_connector_get_client_by_id($values['elasticsearch_watchdog_cluster_id']);
  $index_name = variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX) . '_index';

  if ($client) {
    if ($form['#old_cluster_id'] != '') {
      if ($values['elasticsearch_watchdog_cluster_id'] != $form['#old_cluster_id']) {
        $indeces_status = $client->indices()->status();

        if (!isset($indeces_status['indices'][$index_name])) {
          $result = elasticsearch_watchdog_create_index_and_mapping($client);
          if (empty($result['ok'])) {
            form_set_error('elasticsearch_watchdog_cluster_id', $result['error']);
          }
        }
      }
    }
    else {
      $result = elasticsearch_watchdog_create_index_and_mapping($client);
      if (empty($result['ok'])) {
        form_set_error('elasticsearch_watchdog_cluster_id', $result['error']);
      }
    }
  }
  else {
    form_set_error('elasticsearch_watchdog_cluster_id', t('Cannot connect to the cluster for some reason! Please contact your system administrator.'));
  }
}

/**
 * Create
 *
 * @param array $form
 * @param array $form_state
 */
function elasticsearch_watchdog_settings_submit($form, &$form_state) {
  // TODO: Handle PUT request for the settings.
  // TODO: Handle alias instead of directly indexing and getting from index.
}

function elasticsearch_watchdog_create_index_and_mapping($client) {
  // TODO: Handle settings override because of submit creation the variable is not set.
  $indexParams['index']  =  variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX) . '_index';

  // Index Settings
  $indexParams['body']['settings']['number_of_shards']   = variable_get('elasticsearch_watchdog_num_of_shards', ELASTICSEARCH_WATCHDOG_DEFAULT_SHARDS);
  $indexParams['body']['settings']['number_of_replicas'] = (int)variable_get('elasticsearch_watchdog_num_of_replica', ELASTICSEARCH_WATCHDOG_DEFAULT_REPLICA);

  // Index Mapping
  $myTypeMapping = array(
    '_source' => array(
      'enabled' => true
    ),
    'properties' => array(
      'uid' => array(
        'type' => 'integer',
      ),
      'username' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'type' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'message' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'variables' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'severity' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'link' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'location' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'referer' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'hostname' => array(
        'type' => 'string',
        'index' => 'not_analyzed'
      ),
      'timestamp' => array(
        'type' => 'integer',
      ),
      'date' => array(
        'type' => 'date',
      ),
    )
  );

  $type = variable_get('elasticsearch_watchdog_type',  ELASTICSEARCH_WATCHDOG_DEFAULT_TYPE);
  $indexParams['body']['mappings'][$type] = $myTypeMapping;


  // TODO: Think of building an template for watchdog messages.
  $result = $client->indices()->create($indexParams);

  try {
    $client->indices()->putAlias(array(
      'name' => variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX),
      'index' => variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX) . '_index',
    ));
  }
  catch (Exception $e) {

  }

  return $result;
}

/**
 * @file
* Administrative page callbacks for the Database Logging module.
*/

/**
 * Page callback: Displays a listing of database log messages.
*
* Messages are truncated at 56 chars. Full-length messages can be viewed on the
* message details page.
*
* @see elasticsearch_watchdog_clear_log_form()
* @see elasticsearch_watchdog_event()
* @see elasticsearch_watchdog_filter_form()
* @see elasticsearch_watchdog_menu()
*
* @ingroup logging_severity_levels
*/
function elasticsearch_watchdog_overview() {
  $filter = elasticsearch_watchdog_build_filter_query();

  $rows = array();
  $classes = array(
    WATCHDOG_DEBUG     => 'elasticlog-debug',
    WATCHDOG_INFO      => 'elasticlog-info',
    WATCHDOG_NOTICE    => 'elasticlog-notice',
    WATCHDOG_WARNING   => 'elasticlog-warning',
    WATCHDOG_ERROR     => 'elasticlog-error',
    WATCHDOG_CRITICAL  => 'elasticlog-critical',
    WATCHDOG_ALERT     => 'elasticlog-alert',
    WATCHDOG_EMERGENCY => 'elasticlog-emerg',
  );

  $build['elasticsearch_watchdog_filter_form'] = drupal_get_form('elasticsearch_watchdog_filter_form');
  $build['elasticsearch_watchdog_clear_log_form'] = drupal_get_form('elasticsearch_watchdog_clear_log_form');

  $header = array(
    '', // Icon column.
    array('data' => t('Type'), 'field' => 'type'),
    array('data' => t('Date'), 'field' => 'date', 'sort' => 'desc'),
    t('Message'),
    array('data' => t('User'), 'field' => 'username'),
    array('data' => t('Operations')),
  );

  $client_id = variable_get('elasticsearch_watchdog_cluster_id', '');
  if (!empty($client_id)) {
    $client = elasticsearch_connector_get_client_by_id($client_id);
    if ($client) {
      $params = array();
      $params['index'] = variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX);
      $params['type']  = variable_get('elasticsearch_watchdog_type',  ELASTICSEARCH_WATCHDOG_DEFAULT_TYPE);

      $sort = tablesort_get_sort($header);
      $field = tablesort_get_order($header);
      $limit = variable_get('elasticsearch_watchdog_page_limit', 50);
      $current_page = pager_find_page();
      $params['body']['size']  = $limit;
      $params['body']['from']  = $current_page*$limit;
      $params['body']['sort'][$field['sql']]['order'] = $sort;

      // Filter the results if there are filters specified.
      if (!empty($filter['filter']['and'])) {
        $params['body']['filter'] = $filter['filter'];
      }
      $results = $client->search($params);
      pager_default_initialize($results['hits']['total'], $limit);
    }
  }

  // TODO: Handle sorting and pager!!!
  foreach ($results['hits']['hits'] as $doc) {
    $account = user_load($doc['_source']['uid']);
    $rows[] = array('data' =>
      array(
        // Cells
        array('class' => 'icon'),
        t($doc['_source']['type']),
        format_date($doc['_source']['timestamp'], 'short'),
        theme('elasticsearch_watchdog_message', array('event_id' => $doc['_id'], 'event' => $doc['_source'], 'link' => TRUE)),
        theme('username', array('account' => $account)),
        filter_xss($doc['_source']['link']),
      ),
      // Attributes for tr
      'class' => array(drupal_html_class('elasticlog-' . $doc['_source']['type']), $classes[$doc['_source']['severity']]),
    );
  }

  $build['elasticsearch_watchdog_table'] = array(
    '#theme' => 'table',
    '#header' => $header,
    '#rows' => $rows,
    '#attributes' => array('id' => 'admin-elasticlog'),
    '#empty' => t('No log messages available.'),
  );
  $build['elasticsearch_watchdog_pager'] = array('#theme' => 'pager');

  return $build;
}

/**
 * Page callback: Shows the most frequent log messages of a given event type.
 *
 * Messages are not truncated on this page because events detailed herein do not
 * have links to a detailed view.
 *
 * @param string $type
 *   Type of database log events to display (e.g., 'search').
 *
 * @return array
 *   A build array in the format expected by drupal_render().
 *
 * @see elasticsearch_watchdog_menu()
 */
function elasticsearch_watchdog_top($type) {

  // TODO: Rework the function.

  $header = array(
    array('data' => t('Count'), 'field' => 'count', 'sort' => 'desc'),
    array('data' => t('Path'), 'field' => 'message')
  );

  $result = array();

  $global_facet_name = 'facetname_message';
  $field_faceting = 'message';
  $client_id = variable_get('elasticsearch_watchdog_cluster_id', '');
  if (!empty($client_id)) {
    $client = elasticsearch_connector_get_client_by_id($client_id);
    if ($client) {
      $params = array();
      $params['index'] = variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX);
      $params['type']  = variable_get('elasticsearch_watchdog_type',  ELASTICSEARCH_WATCHDOG_DEFAULT_TYPE);
      $params['search_type'] = 'count';
      $params['body']['query']['term']['type'] = $type;
      $params['body']['facets'][$global_facet_name]['terms']['field'] = $field_faceting;
      $params['body']['facets'][$global_facet_name]['terms']['size'] = variable_get('elasticsearch_watchdog_facet_size', 100);
      $search_result = $client->search($params);
      if (!empty($search_result['facets'])) {
        foreach ($search_result['facets'][$global_facet_name]['terms'] as $facet) {
          $rows[] = array($facet['count'], $facet['term']);
        }
      }
    }
  }

  $build['elasticsearch_watchdog_top_table']  = array(
    '#theme' => 'table',
    '#header' => $header,
    '#rows' => $rows,
    '#empty' => t('No log messages available.'),
  );
  // TODO: Elasticsearch doesn't support pager on facets!
  // $build['elasticsearch_watchdog_top_pager'] = array('#theme' => 'pager');

  return $build;
}

/**
 * Page callback: Displays details about a specific database log message.
 *
 * @param int $id
 *   Unique ID of the database log message.
 *
 * @return array|string
 *   If the ID is located in the Database Logging table, a build array in the
 *   format expected by drupal_render(); otherwise, an empty string.
 *
 * @see elasticsearch_watchdog_menu()
 */
function elasticsearch_watchdog_event($id) {
  $severity = watchdog_severity_levels();
  //$result = db_query('SELECT w.*, u.name, u.uid FROM {watchdog} w INNER JOIN {users} u ON w.uid = u.uid WHERE w.wid = :id', array(':id' => $id))->fetchObject();
  $client_id = variable_get('elasticsearch_watchdog_cluster_id', '');
  if (!empty($client_id)) {
    $client = elasticsearch_connector_get_client_by_id($client_id);
    if ($client) {
      $params = array();
      $params['index'] = variable_get('elasticsearch_watchdog_index', ELASTICSEARCH_WATCHDOG_DEFAULT_INDEX);
      $params['type']  = variable_get('elasticsearch_watchdog_type',  ELASTICSEARCH_WATCHDOG_DEFAULT_TYPE);
      $params['id']    = $id;

      $result = $client->get($params);
      if (!empty($result['exists'])) {
        $source = $result['_source'];
        $account = user_load($source['uid']);
        $rows = array(
          array(
            array('data' => t('Type'), 'header' => TRUE),
            $source['type'],
          ),
          array(
            array('data' => t('Date'), 'header' => TRUE),
            format_date($source['timestamp'], 'long'),
          ),
          array(
            array('data' => t('User'), 'header' => TRUE),
            theme('username', array('account' => $account)),
          ),
          array(
            array('data' => t('Location'), 'header' => TRUE),
            l($source['location'], $source['location']),
          ),
          array(
            array('data' => t('Referrer'), 'header' => TRUE),
            l($source['referer'], $source['referer']),
          ),
          array(
            array('data' => t('Message'), 'header' => TRUE),
            theme('elasticsearch_watchdog_message', array('event_id' => $id, 'event' => $source)),
          ),
          array(
            array('data' => t('Severity'), 'header' => TRUE),
            $severity[$source['severity']],
          ),
          array(
            array('data' => t('Hostname'), 'header' => TRUE),
            check_plain($source['hostname']),
          ),
          array(
            array('data' => t('Operations'), 'header' => TRUE),
            $source['link'],
          ),
        );
        $build['elasticsearch_watchdog_table'] = array(
          '#theme' => 'table',
          '#rows' => $rows,
          '#attributes' => array('class' => array('elasticlog-event')),
        );
        return $build;
      }
    }
  }
  else {
    return '';
  }
}

/**
 * Builds a filter for elasticsearch log administration filters based on session.
 *
 * @return array
 *   An associative array with keys. Only filter for now 'filter'.
 */
function elasticsearch_watchdog_build_filter_query() {
  if (empty($_SESSION['elasticsearch_watchdog_overview_filter'])) {
    return;
  }

  $filters = elasticsearch_watchdog_filters();

  $filter['and'] = array();
  // Build query
  $where = &$filter['and'];
  $args = array();
  foreach ($_SESSION['elasticsearch_watchdog_overview_filter'] as $key => $filter) {
    $filter_where['terms'][$filters[$key]['elastic_key']] = array();
    foreach ($filter as $value) {
      $filter_where['terms'][$filters[$key]['elastic_key']][] = $value;
    }
    if (!empty($filter_where['terms'][$filters[$key]['elastic_key']])) {
      $where['and'][] = $filter_where;
    }
  }

  return array(
    'filter' => $where,
  );
}

/**
 * Creates a list of database log administration filters that can be applied.
 *
 * @return array
 *   Associative array of filters. The top-level keys are used as the form
 *   element names for the filters, and the values are arrays with the following
 *   elements:
 *   - title: Title of the filter.
 *   - where: The filter condition.
 *   - options: Array of options for the select list for the filter.
 */
function elasticsearch_watchdog_filters() {
  $filters = array();

  foreach (_elasticsearch_watchdog_get_message_types() as $key => $type) {
    $types[$key] = t($type);
  }

  if (!empty($types)) {
    $filters['type'] = array(
      'title' => t('Type'),
      'elastic_key' => "type",
      'options' => $types,
    );
  }

  $filters['severity'] = array(
    'title' => t('Severity'),
    'elastic_key' => 'severity',
    'options' => watchdog_severity_levels(),
  );

  return $filters;
}

/**
 * Returns HTML for a log message.
 *
 * @param array $variables
 *   An associative array containing:
 *   - event: An object with at least the message and variables properties.
 *   - link: (optional) Format message as link, event->wid is required.
 *
 * @ingroup themeable
 */
function theme_elasticsearch_watchdog_message($variables) {
  $output = '';
  $event = $variables['event'];
  // Check for required properties.
  if (isset($event['message']) && isset($event['variables'])) {
    // Messages without variables or user specified text.
    if ($event['variables'] === 'N;') {
      $output = $event['message'];
    }
    // Message to translate with injected variables.
    else {
      $output = t($event['message'], unserialize($event['variables']));
    }
    if ($variables['link'] && isset($variables['event_id'])) {
      // Truncate message to 56 chars.
      $output = truncate_utf8(filter_xss($output, array()), 56, TRUE, TRUE);
      $output = l($output, 'admin/reports/elasticlog/elastic-message/' . $variables['event_id'], array('html' => TRUE));
    }
  }
  return $output;
}

/**
 * Form constructor for the database logging filter form.
 *
 * @see elasticsearch_watchdog_filter_form_validate()
 * @see elasticsearch_watchdog_filter_form_submit()
 * @see elasticsearch_watchdog_overview()
 *
 * @ingroup forms
 */
function elasticsearch_watchdog_filter_form($form) {
  $filters = elasticsearch_watchdog_filters();

  $form['filters'] = array(
    '#type' => 'fieldset',
    '#title' => t('Filter log messages'),
    '#collapsible' => TRUE,
    '#collapsed' => empty($_SESSION['elasticsearch_watchdog_overview_filter']),
  );
  foreach ($filters as $key => $filter) {
    $form['filters']['status'][$key] = array(
      '#title' => $filter['title'],
      '#type' => 'select',
      '#multiple' => TRUE,
      '#size' => 8,
      '#options' => $filter['options'],
    );
    if (!empty($_SESSION['elasticsearch_watchdog_overview_filter'][$key])) {
      $form['filters']['status'][$key]['#default_value'] = $_SESSION['elasticsearch_watchdog_overview_filter'][$key];
    }
  }

  $form['filters']['actions'] = array(
    '#type' => 'actions',
    '#attributes' => array('class' => array('container-inline')),
  );
  $form['filters']['actions']['submit'] = array(
    '#type' => 'submit',
    '#value' => t('Filter'),
  );
  if (!empty($_SESSION['elasticsearch_watchdog_overview_filter'])) {
    $form['filters']['actions']['reset'] = array(
      '#type' => 'submit',
      '#value' => t('Reset')
    );
  }
  return $form;
}

/**
 * Form validation handler for elasticsearch_watchdog_filter_form().
 *
 * @see elasticsearch_watchdog_filter_form_submit()
 */
function elasticsearch_watchdog_filter_form_validate($form, &$form_state) {
  if ($form_state['values']['op'] == t('Filter') && empty($form_state['values']['type']) && empty($form_state['values']['severity'])) {
    form_set_error('type', t('You must select something to filter by.'));
  }
}

/**
 * Form submission handler for elasticsearch_watchdog_filter_form().
 *
 * @see elasticsearch_watchdog_filter_form_validate()
 */
function elasticsearch_watchdog_filter_form_submit($form, &$form_state) {
  $op = $form_state['values']['op'];
  $filters = elasticsearch_watchdog_filters();
  switch ($op) {
  	case t('Filter'):
  	  foreach ($filters as $name => $filter) {
  	    if (isset($form_state['values'][$name])) {
  	      $_SESSION['elasticsearch_watchdog_overview_filter'][$name] = $form_state['values'][$name];
  	    }
  	  }
  	  break;
  	case t('Reset'):
  	  $_SESSION['elasticsearch_watchdog_overview_filter'] = array();
  	  break;
  }
  return 'admin/reports/elasticlog';
}

/**
 * Form constructor for the form that clears out the log.
 *
 * @see elasticsearch_watchdog_clear_log_submit()
 * @ingroup forms
 */
function elasticsearch_watchdog_clear_log_form($form) {
  $form['elasticsearch_watchdog_clear'] = array(
    '#type' => 'fieldset',
    '#title' => t('Clear log messages'),
    '#description' => t('This will permanently remove the log messages from the database.'),
    '#collapsible' => TRUE,
    '#collapsed' => TRUE,
  );
  $form['elasticsearch_watchdog_clear']['clear'] = array(
    '#type' => 'submit',
    '#value' => t('Clear log messages'),
    '#submit' => array('elasticsearch_watchdog_clear_log_submit'),
  );

  return $form;
}

/**
 * Form submission handler for elasticsearch_watchdog_clear_log_form().
 */
function elasticsearch_watchdog_clear_log_submit() {
  $_SESSION['elasticsearch_watchdog_overview_filter'] = array();
  // TODO: Delete the messages from the index.
  //db_delete('watchdog')->execute();
  drupal_set_message(t('Elasticsearch logs has been cleared.'));
}