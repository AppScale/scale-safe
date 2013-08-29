# Programmer: Navraj Chohan <nlake44@gmail.com>
import os
import time
import unittest 

import appscale_datastore_batch
import datastore_server

from dbconstants import *

from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_index
from google.appengine.datastore import entity_pb
from google.appengine.datastore import datastore_query

# The separator of the namespace
NAMESPACE_SEP = '/'

# Default DB to use
DB = "cassandra"

class ValidateIDCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    self.app_datastore.ValidateAppId("hi")
  def tearDown(self):
    pass

class GetIndexKeyCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    assert self.app_datastore.GetIndexKey("hi","bye","nye","guy") \
           == "hi/bye/nye/guy"
  def tearDown(self):
    pass

class GetPrefixCase(unittest.TestCase):
  def setUp(self):
    global datastore_batch
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    assert self.app_datastore.GetTablePrefix(('hi1','bye1')) == "hi1/bye1"
  def tearDown(self):
    key = self.app_datastore.GetTablePrefix(("hi1", "bye1"))

class ConfigureNamespaceCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    assert self.app_datastore.GetTablePrefix(("hi","bye")) == "hi/bye"

  def tearDown(self):
    key = self.app_datastore.GetTablePrefix(("hi", "bye"))

class InsertEntityCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    entities = []
    for ii in range(0,10):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name=str(ii),
                                namespace='')
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222",
                     'ccc': "3"*ii})
      entities.append(entity.ToPb())

    self.keys = ['test//TestKind:0!', 
           'test//TestKind:1!', 
           'test//TestKind:2!', 
           'test//TestKind:3!', 
           'test//TestKind:4!', 
           'test//TestKind:5!', 
           'test//TestKind:6!', 
           'test//TestKind:7!', 
           'test//TestKind:8!', 
           'test//TestKind:9!']

    self.app_datastore.InsertEntities(entities)
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    # Verify an entity has been stored
    ret = datastore_batch.batch_get_entity(APP_ENTITY_TABLE, self.keys,
                                           APP_ENTITY_SCHEMA)
    assert 'aaa' in ret['test//TestKind:0!']['entity'] 
    assert 'bbb' in ret['test//TestKind:0!']['entity'] 
    assert 'ccc' in ret['test//TestKind:0!']['entity'] 
    assert 'aaa' in ret['test//TestKind:9!']['entity'] 
    assert 'bbb' in ret['test//TestKind:9!']['entity'] 
    assert 'ccc' in ret['test//TestKind:9!']['entity'] 


  def tearDown(self): 
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    datastore_batch.batch_delete(APP_ENTITY_TABLE, self.keys, APP_ENTITY_SCHEMA)
    datastore_batch.batch_delete(APP_KIND_TABLE, self.keys, APP_KIND_SCHEMA)
    # Verify an entity has been deleted
    ret = datastore_batch.batch_get_entity(APP_ENTITY_TABLE, self.keys,
                                           APP_ENTITY_SCHEMA)
    assert 'entity' not in ret['test//TestKind:0!']

class InsertEntityGroupCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    prev = None
    for ii in range(0,4):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name=str(ii),
                                namespace='b',
                                parent = prev)
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222",
                     'ccc': "3"*ii})
      self.entities.append(entity.ToPb())
    self.keys = ['test/b/TestKind:0!', 
                 'test/b/TestKind:0!TestKind:1!', 
                 'test/b/TestKind:0!TestKind:1!TestKind:2!', 
                 'test/b/TestKind:0!TestKind:1!TestKind:2!TestKind:3!']
    self.kkeys = ['test/b/TestKind:0!', 
                 'test/b/TestKind:1!TestKind:0!', 
                 'test/b/TestKind:2!TestKind:1!TestKind:0!', 
                 'test/b/TestKind:3!TestKind:2!TestKind:1!TestKind:0!']
  def runTest(self):
    self.app_datastore.InsertEntities(self.entities)
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    ret = datastore_batch.batch_get_entity(APP_ENTITY_TABLE, self.keys,
                                           APP_ENTITY_SCHEMA)
     
    assert 'aaa' in ret['test/b/TestKind:0!']['entity'] 
    assert 'bbb' in ret['test/b/TestKind:0!']['entity'] 
    assert 'ccc' in ret['test/b/TestKind:0!']['entity'] 
    assert 'aaa' in ret['test/b/TestKind:0!TestKind:1!']['entity'] 
    assert 'bbb' in ret['test/b/TestKind:0!TestKind:1!']['entity'] 
    assert 'ccc' in ret['test/b/TestKind:0!TestKind:1!']['entity'] 

  def tearDown(self): 
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    ret = datastore_batch.batch_delete(APP_ENTITY_TABLE, self.keys, APP_ENTITY_SCHEMA)
    ret = datastore_batch.batch_delete(APP_KIND_TABLE, self.kkeys, APP_KIND_SCHEMA)
    # Verify an entity has been deleted
    ret = datastore_batch.batch_get_entity(APP_ENTITY_TABLE, self.keys,
                                           APP_ENTITY_SCHEMA)
    assert 'entity' not in ret['test/b/TestKind:0!']

class InsertEntityIndexCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name=str(ii),
                                namespace='a')
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 'bbb': "2222"})

      self.entities.append(entity.ToPb())

    self.keys = ['test/a/TestKind/aaa/1111_2\x00/TestKind:2!', 
                 'test/a/TestKind/bbb/2222\x00/TestKind:2!', 
                 'test/a/TestKind/aaa/1111_0\x00/TestKind:0!', 
                 'test/a/TestKind/bbb/2222\x00/TestKind:0!', 
                 'test/a/TestKind/aaa/1111_1\x00/TestKind:1!', 
                 'test/a/TestKind/bbb/2222\x00/TestKind:1!']
    self.rkeys = ['test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xcd\xff/TestKind:2!', 
                  'test/a/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:2!', 
                  'test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xcc\xff/TestKind:3', 
                  'test/a/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:3!', 
                  'test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xcf\xff/TestKind:0!', 
                  'test/a/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:0!', 
                  'test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xce\xff/TestKind:1!', 
                  'test/a/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:1!']

  def runTest(self):
    self.app_datastore.InsertIndexEntries(self.entities)
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    # Verify an entity has been stored
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'test/a/TestKind:2!' in \
         ret['test/a/TestKind/aaa/1111_2\x00/TestKind:2!']['reference'] 

    ret = datastore_batch.batch_get_entity(DSC_PROPERTY_TABLE, self.rkeys,
                                           PROPERTY_SCHEMA)
    assert 'test/a/TestKind:2!' in \
         ret['test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xcd\xff/TestKind:2!']\
         ['reference']

  def tearDown(self): 
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    ret = datastore_batch.batch_delete(ASC_PROPERTY_TABLE, self.keys, PROPERTY_SCHEMA)
    ret = datastore_batch.batch_delete(DSC_PROPERTY_TABLE, self.rkeys, PROPERTY_SCHEMA)
    # Verify an entity has been deleted
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'reference' not in \
         ret['test/a/TestKind/aaa/1111_2\x00/TestKind:2!']

    ret = datastore_batch.batch_get_entity(DSC_PROPERTY_TABLE, self.rkeys,
                                           PROPERTY_SCHEMA)
    assert 'reference' not in \
         ret['test/a/TestKind/aaa/\xce\xce\xce\xce\xa0\xcd\xff/TestKind:2!']


class InsertGroupEntityIndexCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    prev = None
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name=str(ii),
                                parent = prev,
                                namespace='c')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities.append(entity.ToPb())
    self.keys = ['test/c/TestKind/aaa/1111_0\x00/TestKind:0!', 
                 'test/c/TestKind/bbb/2222\x00/TestKind:0!', 
                 'test/c/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!', 
                 'test/c/TestKind/bbb/2222\x00/TestKind:0!TestKind:1!', 
                 'test/c/TestKind/aaa/1111_2\x00/TestKind:0!TestKind:1!TestKind:2!', 
                 'test/c/TestKind/bbb/2222\x00/TestKind:0!TestKind:1!TestKind:2!']
    self.rkeys = ['test/c/TestKind/aaa/\xce\xce\xce\xce\xa0\xcf\xff/TestKind:0!', 
                  'test/c/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:0!', 
                  'test/c/TestKind/aaa/\xce\xce\xce\xce\xa0\xce\xff/TestKind:0!TestKind:1!', 
                  'test/c/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:0!TestKind:1!', 
                  'test/c/TestKind/aaa/\xce\xce\xce\xce\xa0\xcd\xff/TestKind:0!TestKind:1!TestKind:2!', 
                  'test/c/TestKind/bbb/\xcd\xcd\xcd\xcd\xff/TestKind:0!TestKind:1!TestKind:2!']

  def runTest(self):
    self.app_datastore.InsertIndexEntries(self.entities)
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    # Verify an entity has been stored
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'test/c/TestKind:0!TestKind:1!' in \
         ret['test/c/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']['reference'] 

  def tearDown(self): 
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    ret = datastore_batch.batch_delete(ASC_PROPERTY_TABLE, self.keys, PROPERTY_SCHEMA)
    ret = datastore_batch.batch_delete(DSC_PROPERTY_TABLE, self.rkeys, PROPERTY_SCHEMA)
    # Verify an entity has been deleted
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'reference' not in \
         ret['test/c/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']

class AllocateIDsCase(unittest.TestCase):
  def setUp(self): 
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    datastore_batch.batch_delete(APP_ID_TABLE, ["a/a"], APP_ID_SCHEMA)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
  def runTest(self):
    s, e = self.app_datastore.AllocateIds("a/a", 1000)
    assert s == 10000 and e == 10999
    for ii in range (0,20):
      s, e = self.app_datastore.AllocateIds("a/a", 500)
    assert s == 21000 and e == 21499 

  def tearDown(self):
    pass 

class InsertAndDeleteIndexesCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    prev = None
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name=str(ii),
                                parent = prev,
                                namespace='d')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities.append(entity.ToPb())
    self.keys = ['test/d/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']

  def runTest(self):
    self.app_datastore.InsertIndexEntries(self.entities)
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'test/d/TestKind:0!TestKind:1!' in \
         ret['test/d/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']['reference'] 

    self.app_datastore.DeleteIndexEntries(self.entities)
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'reference' not in \
         ret['test/d/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']

  def tearDown(self):
    pass

class PutCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    prev = None
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name = str(ii),
                                parent = prev,
                                namespace='e')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities.append(entity.ToPb())

    self.entities2 = []
    prev = None
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name = str(ii),
                                parent = prev,
                                namespace='e')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "x111_" + str(ii), 
                     'bbb': "x222"})
      self.entities2.append(entity.ToPb())

    tuples = sorted((self.app_datastore.GetTablePrefix(x), x) for x in self.entities)

    # keys should be the same for entities and entities2
    self.keys = self.app_datastore.GetIndexKVFromTuple(tuples, reverse=False)
    self.keys = [x[0] for x in self.keys]
    tuples = sorted((self.app_datastore.GetTablePrefix(x), x) for x in self.entities2)

    # keys should be the same for entities and entities2
    self.keys2 = self.app_datastore.GetIndexKVFromTuple(tuples, reverse=False)
    self.keys2 = [x[0] for x in self.keys2]

  def runTest(self):
    self.app_datastore.PutEntities(self.entities)
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys,
                                           PROPERTY_SCHEMA)
    assert 'test/e/TestKind:0!TestKind:1' in \
         ret['test/e/TestKind/aaa/1111_1\x00/TestKind:0!TestKind:1!']['reference'] 
    # overwrite test
    self.app_datastore.PutEntities(self.entities2)
    ret = datastore_batch.batch_get_entity(ASC_PROPERTY_TABLE, self.keys2,
                                           PROPERTY_SCHEMA)
    assert 'test/e/TestKind:0!TestKind:1' in \
         ret['test/e/TestKind/aaa/x111_1\x00/TestKind:0!TestKind:1!']['reference'] 

  def tearDown(self):
    keys = [e.key() for e in self.entities] 
    self.app_datastore.DeleteEntities(keys)

class GetCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    self.keys = []
    prev = None
    for ii in range(0,3):
      entity = datastore.Entity("TestKind", 
                                _app="test",
                                name = str(ii),
                                parent = prev,
                                namespace='e')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities.append(entity.ToPb())
    self.keys = [e.key() for e in self.entities] 
    self.app_datastore.PutEntities(self.entities)
  def runTest(self):
    results, keys = self.app_datastore.FetchKeys(self.keys)
    self.app_datastore.DeleteEntities(self.keys)
    results, keys = self.app_datastore.FetchKeys(self.keys)
    for ii in results:
      if 'entity' in ii: raise
 
  def tearDown(self):
    pass 

class KindQueryCase(unittest.TestCase):
  def setUp(self):
    datastore_batch = appscale_datastore_batch.DatastoreFactory.getDatastore(DB)
    self.app_datastore = datastore_server.DatastoreDistributed(datastore_batch) 
    self.entities = []
    self.keys = []
    self.entities2 = []
    self.keys2 = []

    prev = None
    for ii in range(0,3):
      entity = datastore.Entity(kind="ATestKind", 
                                _app="test",
                                name = str(ii),
                                parent = prev,
                                namespace='f')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities.append(entity.ToPb())
    self.keys = [e.key() for e in self.entities] 
    self.app_datastore.PutEntities(self.entities)

    prev = None
    for ii in range(0,3):
      entity = datastore.Entity(kind="BTestKind", 
                                _app="test",
                                name = str(ii),
                                parent = prev,
                                namespace='f')
      prev = entity
      # have properties with different values bye same property names
      entity.update({'aaa': "1111_" + str(ii), 
                     'bbb': "2222"})
      self.entities2.append(entity.ToPb())
    self.keys2 = [e.key() for e in self.entities2] 
    self.app_datastore.PutEntities(self.entities2)

  def runTest(self):
    def testKind(kind):
      q = datastore.Query(kind=kind, _app="test", namespace='f')
      q = q._ToPb()

      result = self.app_datastore.KindQuery(q, [], [])
    
      for ii in result:
        item = entity_pb.EntityProto(ii)
        for ii in item.entity_group().element_list():
          assert kind == ii.type()
    testKind("ATestKind")
    testKind("BTestKind") 

    q = datastore.Query(kind="ATestKind", _app="test", namespace='f')
    q = q._ToPb()
    q.set_limit(1)

    result = self.app_datastore.KindQuery(q, [], [])
    last_item = entity_pb.EntityProto(result[0])
    last_item = last_item.key() 

    q = datastore.Query(kind="ATestKind", _app="test", namespace='f')
    q = q._ToPb()
    f = q.add_filter() 
    #entity_pb.Property(last_item)
    #filt = datastore_query.PropertyFilter(">", last_item)
    #print filt 

  def tearDown(self):
    self.app_datastore.DeleteEntities(self.keys)
    self.app_datastore.DeleteEntities(self.keys2)


if __name__ == "__main__":
  unittest.main() 
