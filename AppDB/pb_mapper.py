##########################################################################
# 
# APPSCALE CONFIDENTIAL
# __________________
# 
#  [2012] - [2013] AppScale Systems Incorporated 
#  All Rights Reserved.
# 
# NOTICE:  All information contained herein is, and remains
# the property of AppScale Systems Incorporated and its suppliers,
# if any.  The intellectual and technical concepts contained
# herein are proprietary to AppScale Systems Incorporated
# and its suppliers and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination of this information or reproduction of this material
# is strictly forbidden unless prior written permission is obtained
# from AppScale Systems Incorporated.
# 
##########################################################################

""" Converts Google Cloud Datastore (GCD) protocol buffers into 
App Engine datastore protocol buffers and vice versa.

See https://developers.google.com/datastore/docs/apis/v1beta1/proto
for mapping.
 
"""
import googledatastore
from google.appengine.datastore import datastore_pb
from google.appengine.datastore.entity_pb import Property

import base64
import logging
import os
import uuid

class ValueType:
  """ Different supported types of values for entity properties. """
  INT = 0
  STRING = 1
  BOOL = 2
  DOUBLE = 3
  POINT = 4
  USER = 5
  REFERENCE = 6
  DATE = 7
  BLOB_KEY = 8
  TEXT = 9
  BLOB_STRING = 10

class PbMapper():
  """ This class maps protocol buffers between Google App Engine
  datastore protocol buffers and Google Cloud Datastore protocol buffers. 
  """

  # App Engine protocol buffer code to tell Google Cloud Datastore this 
  # field is holding user information.
  GCD_USER = 20

  def __init__(self, app_id="", dataset="", service_email="", private_key=""):
    """ Constructs a new PbMapper instance. 
 
    Args:
      app_id: A str, the application identifier.
      dataset: A str, the Google Cloud Datastore dataset identifier.
    """
    self.app_id = app_id

    if service_email and private_key:
      logging.info("Service email set to: %s" % service_email)
      logging.info("Private key is at: %s" % private_key)
      self.__service_email = service_email
      self.__private_key = private_key
      self._set_environ()

    googledatastore.set_options(dataset=dataset)

  def _set_environ(self):
    """ Sets the environment variables required by the Google Cloud
    Datastore client.
    """
    os.environ['DATASTORE_SERVICE_ACCOUNT'] = self.__service_email
    os.environ['DATASTORE_PRIVATE_KEY_FILE'] = self.__private_key

  def get_property_value(self, prop):
    """ Gets the value of a property and its type.

    Args:
      prop: A entity_pb.PropertyValue object.
    Returns:
      A tuple of (ValueType, value).
    """
    value = None
    if prop.has_value():
      value = prop.value() 
    else:
      return (None, None)

    if value.has_int64value():
      if prop.has_meaning() and prop.meaning() == Property.GD_WHEN:
        return (ValueType.DATE, value.int64value())
      else:
        return (ValueType.INT, value.int64value())
    elif value.has_booleanvalue():
      return (ValueType.BOOL, value.booleanvalue())
    elif value.has_doublevalue():
      return (ValueType.DOUBLE, value.doublevalue())
    elif value.has_stringvalue():
      if prop.has_meaning() and prop.meaning() == Property.BLOBKEY:
        return (ValueType.BLOB_KEY, value.stringvalue())
      elif prop.has_meaning() and prop.meaning() == Property.TEXT:
        return (ValueType.TEXT, value.stringvalue())
      elif prop.has_meaning() and prop.meaning() == Property.BYTESTRING:
        return (ValueType.BLOB_STRING, value.stringvalue())
      else:
        return (ValueType.STRING, value.stringvalue())
    elif value.has_pointvalue():
      return (ValueType.POINT, value.pointvalue())
    elif value.has_uservalue():
      return (ValueType.USER, value.uservalue())
    elif value.has_referencevalue():
      return (ValueType.REFERENCE, value.referencevalue())
    else:
      return (None, None)

  def send_blind_write(self, request):
    """ Sends a blind write request to Google Cloud Datastore. 

    This is not transactional.

    Args:
      request: A Google Cloud Datastore BlindWrite request.
    Returns:
      A Google Cloud Datastore BlindWrite response.
    """
    return googledatastore.blind_write(request)

  def set_properties(self, gcd_entity, property_list, is_raw=False):
    """ Sets the properties for a given Google Cloud Datastore entity. 

    Args:
      gcd_entity: A Google Cloud Datastore Entity instance.
      property_list: A list of properties to place into the gcd_entity.
      is_raw: True if we do not index the properties.
    """
    # For properties that have multiple values.
    list_reference_cache = {}

    # Set up the property lists. 
    for prop in property_list:
      gcd_prop = None
      if prop.name() in list_reference_cache:
        gcd_prop = list_reference_cache[prop.name()]
      else:
        gcd_prop = gcd_entity.property.add()
        gcd_prop.name = prop.name()
        list_reference_cache[prop.name()] = gcd_prop

      # For db.ListProperty and ndb repeated support.
      # Defaults to false if not set.
      if prop.has_multiple() and prop.multiple() == True:
        gcd_prop.multi = prop.multiple()

      prop_type, prop_value = self.get_property_value(prop)
      value = None
      if prop_type == ValueType.INT:
        value = gcd_prop.value.add()
        value.integer_value = prop_value
      elif prop_type == ValueType.DATE:
        value = gcd_prop.value.add()
        value.timestamp_microseconds_value = prop_value
      elif prop_type == ValueType.STRING:
        value = gcd_prop.value.add()
        value.string_value = prop_value
      elif prop_type == ValueType.BLOB_KEY:
        value = gcd_prop.value.add()
        value.blob_key_value = prop_value
      elif prop_type == ValueType.BLOB_STRING:
        value = gcd_prop.value.add()
        value.blob_value = prop_value
      elif prop_type == ValueType.DOUBLE:
        value = gcd_prop.value.add()
        value.double_value = prop_value
      elif prop_type == ValueType.BOOL:
        value = gcd_prop.value.add()
        value.boolean_value = prop_value
      elif prop_type == ValueType.REFERENCE:
        value = gcd_prop.value.add()
        value.indexed = False
        key = value.entity_value.key
        for element in prop_value.pathelement_list():
          new_element = key.path_element.add()
          new_element.kind = element.type()
          if element.has_name():
            new_element.name = element.name()
          else:
            new_element.id = element.id()
      elif prop_type == ValueType.POINT:
        point_value = gcd_prop.value.add()
        point_value.meaning = Property.GEORSS_POINT

        langitude = point_value.entity_value.property.add() 
        langitude.name = "x"
        lang_value = langitude.value.add()
        lang_value .double_value = prop_value.x()
        lang_value.indexed = False

        longitude = point_value.entity_value.property.add() 
        longitude.name = "y"
        long_value = longitude.value.add()
        long_value.double_value = prop_value.y()
        long_value.indexed = False
      elif prop_type == ValueType.TEXT:
        value = gcd_prop.value.add()
        value.string_value = prop_value
        value.indexed = False
        value.meaning = Property.TEXT
      elif prop_type == ValueType.USER:
        value = gcd_prop.value.add()
        value.meaning = self.GCD_USER
        value.indexed = False

        # Set the auth domain for this user.
        if prop_value.has_auth_domain():
          auth_domain_prop = value.entity_value.property.add() 
          auth_domain_prop.name = "auth_domain"
          auth_domain_value = auth_domain_prop.value.add()
          auth_domain_value.indexed = False
          auth_domain_value.string_value = prop_value.auth_domain()

        # Set the email for this user.
        if prop_value.has_email():
          email_prop = value.entity_value.property.add()
          email_prop.name = "email"
          email_value = email_prop.value.add()
          email_value.indexed = False
          email_value.string_value = prop_value.email()

        # Set the user id.
        if prop_value.has_gaiaid():
          user_id_prop = value.entity_value.property.add()
          user_id_prop.name = "user_id"
          user_value = user_id_prop.value.add()
          user_value.indexed = False
          # We cast the gaiaid as string. 
          user_value.string_value = str(prop_value.gaiaid())

      else:
        # Create an empty value.
        gcd_prop.value.add()

      if is_raw:
        value.indexed = False

  def convert_blind_put_request(self, request):
    """ Converts a datastore_pb.PutRequest to a Google Cloud Datastore
    blind update request.

    Args:
      request: A datastore_pb.PutRequest.
    Returns:
      A googledatastore.BlindWriteRequest mapped over from the given PutRequest.
    """
    blind_req = googledatastore.BlindWriteRequest()
    mutations = blind_req.mutation

    entities = request.entity_list()

    # This for loop sets the key path of each entity.
    for entity in entities:
      gcd_entity = None
      # Before we build the path we must first know if the full path
      # is already set, or if we need to auto assign an ID for this
      # new entity.
      last_path = entity.key().path().element_list()[-1]
      if last_path.has_name() or last_path.id() != 0:
        gcd_entity = mutations.upsert.add()
      else:
        gcd_entity = mutations.insert_auto_id.add()

      # Loop through the ancestors (element) of this entity's path.
      for element in entity.key().path().element_list():
        path_element = gcd_entity.key.path_element.add()
        path_element.kind = element.type()
        if element.has_name():
          path_element.name = element.name()
        elif element.id() != 0:
          path_element.id = element.id() 

      self.set_properties(gcd_entity, entity.property_list(), is_raw=False)
      self.set_properties(gcd_entity, entity.raw_property_list(), is_raw=True)
    return blind_req

  def convert_blind_put_response(self, put_request, gcd_response,
    put_response):
    """ Converts a Google Cloud Datastore blind put response to a 
    datastore_pb.PutResponse.
  
    Args:
      request: datastore_pb.PutRequest, used to construct keys which are not
        returned in the mutation result (any keys which had a full ancestor 
        path).
      gcd_response: googledatatore.MutationResult returned from Google 
        Cloud Datastore.
      put_response: A datastore_pb.PutResponse reference to fill in.
    Returns:
      Filled in put_response, a datastore_pb.PutResponse.
    """
    # First get all the keys from the put_request because not all mutation
    # results are returned by Google Cloud Datastore.
    for entity in put_request.entity_list():
      if entity.has_key:
        # We only want to add the key if it has a full path. We ignore any
        # that are missing both the name and id because those will be 
        # in the mutation result from GCD.
        key = entity.key()
        path = key.path()
        last_element = path.element_list()[-1]
        if last_element.id() == 0 and not last_element.has_name():
          continue
        else:
          new_key = put_response.add_key()
          new_key.set_app(self.app_id)
          for element in path.element_list():
            new_element = new_key.mutable_path().add_element()
            new_element.set_type(element.type())
            if element.has_id():
              new_element.set_id(element.id())
            elif element.has_name():
              new_element.set_name(element.name())
            else:
              raise TypeError("Element has neither a name or ID")
    try:
      if gcd_response.HasField("mutation_result"):
        for key in gcd_response.mutation_result.insert_auto_id_key:
          new_key = put_response.add_key()
          new_key.set_app(self.app_id)
          for path_element in key.path_element:
            new_element = new_key.mutable_path().add_element()
            new_element.set_id(path_element.id)
            new_element.set_type(path_element.kind)
    except ValueError:
      # There were no auto ID insertions.
      pass

    return put_response

  def send_lookup(self, request): 
    """ Sends a Google Cloud Datastore Lookup and returns the resposne.

    Args:
      request: A Google Cloud Datastore LookupRequest.
    Returns:
      A Google Cloud Datastore LookupResponse. 
    """
    return googledatastore.lookup(request)

  def convert_get_request(self, request):
    """ Converts a datastore_pb.GetRequest to a Google Cloud Datastore
    lookup request. 

    Args:
      request: A datastore_pb.GetRequest.
    Returns:
      A Google Cloud Datastore lookup protocol message.
    """
    gcd_lookup = googledatastore.LookupRequest()

    if request.has_transaction():
      gcd_lookup.read_options.transaction = \
        request.transaction().handle()

    for key in request.key_list():
      new_key = gcd_lookup.key.add()
      for element in key.path().element_list():
        new_element = new_key.path_element.add()
        new_element.kind = element.type()
        if element.id() != 0:
          new_element.id = element.id()
        elif element.has_name():
          new_element.name = element.name()
    return gcd_lookup

  def convert_get_response(self, response, get_response):
    """ Converts a Google Cloud Datastore LookupResponse to a 
    datastore_pb.GetResponse.

    Args:
      response: A Google Cloud Datastore LookupResponse.
      get_response: A datastore_pb.GetResponse reference.
    Returns:
      A datastore_pb.GetResponse filled in with entity information from
      the Google Cloud Datastore object.
    """
    try:
      for entity_result in response.missing:
        get_response.add_entity()
    except ValueError, value_error:
      logging.exception(value_error) 

    try:
      for entity_result in response.found:
        new_entity = get_response.add_entity().mutable_entity()
        new_key = new_entity.mutable_key()
        new_key.set_app(self.app_id)
        new_path = new_key.mutable_path()
        entity = entity_result.entity
 
        # Set the key for the entity.
        key = entity.key 
        for path_element in key.path_element:
          new_element = new_path.add_element()
          new_element.set_type(path_element.kind)
          if path_element.HasField("name"):
            new_element.set_name(path_element.name)
          else:
            new_element.set_id(path_element.id)

        # Set the entity group this entity belongs to.
        ent_group_path = new_entity.mutable_entity_group() 
        first_path_element = key.path_element[0]
        new_first_path_element = ent_group_path.add_element()
        new_first_path_element.set_type(first_path_element.kind)
        if first_path_element.HasField("name"):
          new_first_path_element.set_name(first_path_element.name)
        else:
          new_first_path_element.set_id(first_path_element.id)

        self.add_properties_to_entity_pb(new_entity, entity)

    except ValueError, value_error:
      logging.exception(value_error)

    return get_response

  def convert_delete_request_to_blind_write(self, request):
    """ Converts a datastore_pb.DeleteRequest to a Google Cloud Datastore
    blind write delete mutation request.
    
    Args:
      request: A datastore_pb.DeleteRequest object.
    Returns:
      A Google Cloud Datatore BlindWrite request with delete mutations. 
    """
    blind_write_req = googledatastore.BlindWriteRequest()
    key_list = []
    for key in request.key_list():
      new_key = googledatastore.Key()
      for element in key.path().element_list():
        new_element = new_key.path_element.add()
        new_element.kind = element.type()
        if element.id() != 0:
          new_element.id = element.id()
        elif element.has_name():
          new_element.name = element.name()
        else:
          raise TypeError("Missing ID or name of key") 
      key_list.append(new_key)
    blind_write_req.mutation.delete.extend(key_list)
    return blind_write_req

  def convert_begin_transaction_request(self, request):
    """ Converts a datastore_pb.BeginTransactionRequest to a Google Cloud 
    Datastore begin request.
 
    Args:
      request: A datastore_pb.BeginTransactionRequest.
    Returns:
      A googledatastore.BeginTransactionRequest.
    """
    return googledatastore.BeginTransactionRequest()

  def send_begin_transaction_request(self, request):
    """ Sends a Google Cloud Datastore begin transaction request.
  
    Args:
      request: A googledatastore.BeginTransactionRequest.
    Returns:
      A googledatastore.BeginTransactionResponse.
    """
    return googledatastore.begin_transaction(request)

  def convert_begin_transaction_response(self, response):
    """ Converts a Google Cloud Datastore begin response to a
    datastore_pb.BeginResponse.

    Args:
      response: datastore_pb.Transaction response object.
    """
    transaction_pb = datastore_pb.Transaction()
    transaction_pb.set_app(self.app_id)
    transaction_pb.set_handle(response.transaction)
    return transaction_pb 

  def convert_rollback_request(self, request):
    """ Converts a Google App Engine rollback request to a Google Cloud 
    Datastore rollback request.

    Args:
      request: A datastore_pb.Transaction.
    Returns:
      A googledatastore.RollbackRequest.
    """
    gcd_rollback_request = googledatastore.RollbackRequest()
    gcd_rollback_request.transaction = request.handle()
    return gcd_rollback_request

  def send_rollback_request(self, request):
    """ Sends a rollback request to Google Cloud Datastore service.

    Args:
      request: A googledatastore.RollbackRequest.
    """
    googledatastore.rollback(request)

  def fill_in_key(self, new_key, element_list):
    """ Fills in key with a path element list.
  
    Args:
      new_key: A Google Cloud Datastore Key.
      element_list: A list of entity_pb.PropertyValue_ReferenceValue.
    """
    for element in element_list:
      new_element = new_key.path_element.add()
      new_element.kind = element.type()
      if element.has_name():
        new_element.name = element.name()
      else:
        new_element.id = element.id()

  def convert_query_request(self, request):
    """ Converts a datastore_pb.QueryRequest to a Google Cloud Datastore 
    request.

    Args: 
      request: A datastore_pb.QueryRequest.
    Returns:
      The mapped over Google Cloud Datastore QueryResult.
    """
    gcd_run_query_request = googledatastore.RunQueryRequest()
    gcd_query = gcd_run_query_request.query

    if request.has_transaction():
      gcd_run_query_request.read_options.transaction = \
        request.transaction().handle()

    #TODO projection queries
    if request.has_kind():
      kind_expression = gcd_query.kind.add()
      kind_expression.name = request.kind()

    # Add query order to the query.
    for order in request.order_list():
      new_order = gcd_query.order.add()
      new_order.property.name = order.property()
      if order.has_direction():
        new_order.direction = order.direction()

    if request.has_offset():
      gcd_query.offset = request.offset()
  
    if request.has_limit():
      gcd_query.limit = request.limit()

    if request.has_compiled_cursor():
      # We place the GCD cursor in the second position of the compiled
      # cursors. This gives us backward compatibility with AppScale.
      start_key = request.compiled_cursor().position_list()[1].start_key()
      gcd_query.start_cursor = start_key

    if request.has_end_compiled_cursor():
      start_key = request.end_compiled_cursor().position_list()[1].start_key()
      gcd_query.end_cursor = start_key

    if request.filter_size() == 0 and not request.has_ancestor():
      return gcd_run_query_request

    # We add all filters to a composite list.
    query_filters = gcd_query.filter
    query_filters.composite_filter.operator = \
      googledatastore.CompositeFilter.AND
    for filter in request.filter_list():
      for prop in filter.property_list():
        new_filter = query_filters.composite_filter.filter.add()
        prop_filter = new_filter.property_filter
        prop_filter.operator = filter.op()
        prop_filter.property.name = prop.name()
        value = prop.value()
        if value.has_int64value():
          prop_filter.value.integer_value = value.int64value()
        elif value.has_booleanvalue():
          prop_filter.value.boolean_value = value.booleanvalue()
        elif value.has_stringvalue():
          prop_filter.value.string_value = value.stringvalue()
        elif value.has_doublevalue():
          prop_filter.value.double_value = value.doublevalue()
        elif value.has_referencevalue():
          element_list = value.referencevalue().pathelement_list()
          self.fill_in_key(prop_filter.value.key_value, element_list)
        else:
          raise TypeError("Unable to get a value for property--wrong type") 
   
    # Put in ancestor filters if the query has an ancestor specified.
    if request.has_ancestor():
      ancestor_key = googledatastore.Key()
      self.fill_in_key(ancestor_key, request.ancestor().path().element_list())
      key_filter = query_filters.composite_filter.filter.add()
      prop_filter = key_filter.property_filter
      prop_filter.property.name = '__key__'
      prop_filter.operator = googledatastore.PropertyFilter.HAS_ANCESTOR
      prop_filter.value.key_value.CopyFrom(ancestor_key)
  
    return gcd_run_query_request

  def send_query(self, request):
    """ Does a RPC call to Google Cloud Datastore for a query.
    
    Args:
      request: A Google Cloud Datastore Query protocol buffer.
    Returns:
      A Google Cloud Datastore QueryResults protcol buffer.
    """
    return googledatastore.run_query(request)

  def add_properties_to_entity_pb(self, new_entity, gcd_entity):
    """ Adds a property to an entity object from a Google Cloud Datastore
    entity protocol buffer.

    Args:
      new_entity: A entity_pb.EntityProto reference.
      gcd_entity: A Google Cloud Datastore entity item.
    """
    for prop in gcd_entity.property:
      for value in prop.value:
        new_property = new_entity.add_property()
        new_property.set_name(prop.name)
        new_property.set_multiple(prop.multi)
        new_value = new_property.mutable_value()

        if value.HasField("meaning"):
          new_property.set_meaning(value.meaning)
  
        if value.HasField("boolean_value"):
          new_value.set_booleanvalue(value.boolean_value)
        elif value.HasField("integer_value"):
          new_value.set_int64value(value.integer_value)
        elif value.HasField("double_value"):
          new_value.set_doublevalue(value.double_value)
        elif value.HasField("timestamp_microseconds_value"):
          new_property.set_meaning(Property.GD_WHEN)
          new_value.set_int64value(value.timestamp_microseconds_value)
        elif value.HasField("key_value"):
          new_reference_value = new_value.mutable_referencevalue()
          new_reference_value.set_app(self.app_id)
          for element in value.key_value.path_element:
            new_element = new_reference_value.add_pathelement()
            new_element.set_type(element.kind)
            if element.HasField("id"):
              new_element.set_id(element.id)
            else:
              new_element.set_name(element.name)
        elif value.HasField("blob_key_value"):
          new_value.set_stringvalue(value.blob_key_value)
        elif value.HasField("string_value"):
          new_value.set_stringvalue(value.string_value)
        elif value.HasField("blob_value"):
          new_property.set_meaning(Property.BYTESTRING)
          new_value.set_stringvalue(value.blob_value)
        elif value.HasField("entity_value"):
          if value.meaning == self.GCD_USER:
            new_user = new_value.mutable_uservalue()
            for user_property in value.entity_value.property:
              if user_property.name == "email":
                new_user.set_email(user_property.value.string_value)
              if user_property.name == "auth_domain":
                new_user.set_auth_domain(user_property.value.string_value)
              if user_property.name == "user_id":
                new_user.set_gaiaid(user_property.value.string_value)
          elif value.meaning == Property.GEORSS_POINT:
            new_point = new_value.mutable_pointvalue()
            for point_property in value.entity_value.property:
              if point_property.name == "x":
                new_point.set_x(value.double_value)
              if point_property.name == "y":
                new_point.set_y(value.double_value)
  
  def convert_query_response(self, response, query_result):
    """ Converts a Google Cloud Datastore RunQueryResponse to a 
    datastore_pb.QueryResponse.
    
    Args:
      response: A googledatastore RunQueryResponse.
      query_result: A datastore_pb.QueryResponse.
    Returns:
      A filled in a datastore_pb.QueryResponse.
    """
    if not response.HasField("batch"):
      query_result.set_more_results(False)
      mutable_cursor = query_result.mutable_cursor()
      return

    if googledatastore.QueryResultBatch.NO_MORE_RESULTS == \
      response.batch.more_results:
      query_result.set_more_results(False)
    else:
      query_result.set_more_results(True)

    if response.batch.HasField('skipped_results'): 
      query_result.set_skipped_results(response.batch.skipped_results)
 
    result_type = response.batch.entity_result_type
    if result_type == googledatastore.EntityResult.KEY_ONLY:
      query_result.set_keys_only(True)

    mutable_cursor = query_result.mutable_cursor()

    if response.batch.HasField('end_cursor'): 
      compiled_cursor = query_result.mutable_compiled_cursor()
      position = compiled_cursor.add_position() 
      position.set_start_key(response.batch.end_cursor)
      mutable_cursor.set_app(self.app_id)
      mutable_cursor.set_cursor(uuid.uuid1().int>>64)

    for entity_result in response.batch.entity_result:
      new_entity = query_result.add_result()
      new_key = new_entity.mutable_key()
      new_key.set_app(self.app_id)
      new_path = new_key.mutable_path()
      for element in entity_result.entity.key.path_element:
        new_element = new_path.add_element() 
        new_element.set_type(element.kind)
        if element.HasField("name"):
          new_element.set_name(element.name)
        else:
          new_element.set_id(element.id)

      # Set the entity group of this entity.
      first_element = entity_result.entity.key.path_element[0]
      entity_group = new_entity.mutable_entity_group() 
      new_first_element = entity_group.add_element()
      new_first_element.set_type(first_element.kind)
      if first_element.HasField("name"):
        new_first_element.set_name(first_element.name)
      else:
        new_first_element.set_id(first_element.id)

      # Skip adding properties if its a keys only query.
      if not query_result.keys_only():
        self.add_properties_to_entity_pb(new_entity, entity_result.entity)

    return query_result

  def convert_allocate_id_request(self, request):
    """ Converts a datastore_pb.AllocateIdsRequest to a Google Cloud 
    allocate ids request.
   
    Args:
      request: A datastore_pb.AllocateIdsRequest.
    Returns:
      A Google Cloud Datastore AllocateIdsRequest converted over from
      a Google App Engine allocate ID request.
    """
    raise NotImplementedError()

  def create_commit_request(self, transaction, put_entities, delete_keys):
    """ Creates a commit request to be sent to Google Cloud Datastore.

    Args:
      request: A datastore_pb.Transaction.
      put_entities: A list of entity_pb.EntityProtos.
      delete_keys: A list of entity_pb.References.
    Returns:
      A googledatastore.CommitRequest.
    Raise:
      A TypeError if paths are partial (neither id nor name for entity).
    """
    gcd_commit = googledatastore.CommitRequest()
    gcd_commit.transaction = transaction.handle()
    mutations = gcd_commit.mutation

    for entity in put_entities:
      gcd_entity = None
      # Before we build the path we must first know if the full path
      # is already set, or if we need to auto assign an ID for this
      # new entity.
      last_path = entity.key().path().element_list()[-1]
      if not last_path.has_name() and last_path.id() == 0:
        raise TypeError("Element has neither a name or ID")

      gcd_entity = mutations.upsert.add()
      # Loop through the ancestors (element) of this entity's path.
      for element in entity.key().path().element_list():
        path_element = gcd_entity.key.path_element.add()
        path_element.kind = element.type()
        if element.has_name():
          path_element.name = element.name()
        elif element.id() != 0:
          path_element.id = element.id() 

      self.set_properties(gcd_entity, entity.property_list(), is_raw=False)
      self.set_properties(gcd_entity, entity.raw_property_list(), is_raw=True)
     
    for key in delete_keys:
      new_delete = mutations.delete.add()
      self.fill_in_key(new_delete, key.path().element_list())

    return gcd_commit

  def send_commit(self, commit):
    """ Sends a commit message to Google Cloud Datastore.

    Args:
      commit: A googledatastore.CommitRequest.
    Returns:
      A googledatastore.CommitResponse.
    """
    return googledatastore.commit(commit)
