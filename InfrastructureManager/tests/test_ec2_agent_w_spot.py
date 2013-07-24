from agents.factory import InfrastructureAgentFactory
from boto.ec2.connection import EC2Connection
from boto.ec2.instance import Reservation, Instance
from boto.ec2.keypair import KeyPair
from boto.ec2.securitygroup import SecurityGroup
from boto.exception import EC2ResponseError
from flexmock import flexmock
from infrastructure_manager import InfrastructureManager
import time
from utils import utils
try:
  from unittest import TestCase
except ImportError:
  from unittest.case import TestCase


__author__ = 'hiranya'
__email__ = 'hiranya@appscale.com'

class TestEC2Agent(TestCase):

  def test_ec2_run_instances(self):
    i = InfrastructureManager(blocking=True)

    # first, validate that the run_instances call goes through successfully
    # and gives the user a reservation id
    full_params = {
      'credentials': {'a': 'b', 'EC2_URL': 'http://testing.appscale.com:8773/foo/bar',
                      'EC2_ACCESS_KEY': 'access_key', 'EC2_SECRET_KEY': 'secret_key'},
      'group': 'boogroup',
      'image_id': 'booid',
      'infrastructure': 'ec2',
      'instance_type': 'booinstance_type',
      'keyname': 'bookeyname',
      'num_vms': '1',
      'use_spot_instances': 'True',
      'max_spot_price' : '1.23'
    }

    id = '0000000000'  # no longer randomly generated
    full_result = {
      'success': True,
      'reservation_id': id,
      'reason': 'none'
    }
    self.assertEquals(full_result, i.run_instances(full_params, 'secret'))

    # next, look at run_instances internally to make sure it actually is
    # updating its reservation info
    self.assertEquals(InfrastructureManager.STATE_RUNNING, i.reservations.get(id)['state'])
    vm_info = i.reservations.get(id)['vm_info']
    self.assertEquals(['public-ip'], vm_info['public_ips'])
    self.assertEquals(['private-ip'], vm_info['private_ips'])
    self.assertEquals(['i-id'], vm_info['instance_ids'])

  def setUp(self):
    (flexmock(EC2Connection)
      .should_receive('get_key_pair')
      .and_return(None))
    (flexmock(EC2Connection)
      .should_receive('create_key_pair')
      .with_args('bookeyname')
      .and_return(KeyPair()))
    (flexmock(EC2Connection)
      .should_receive('get_all_security_groups')
      .and_return([]))
    (flexmock(EC2Connection)
      .should_receive('create_security_group')
      .with_args('boogroup', 'AppScale security group')
      .and_return(SecurityGroup()))
    (flexmock(EC2Connection)
      .should_receive('authorize_security_group')
      .and_return())
    reservation = Reservation()
    instance = Instance()
    instance.private_dns_name = 'private-ip'
    instance.public_dns_name = 'public-ip'
    instance.id = 'i-id'
    instance._state.name = 'running'
    instance.key_name = 'bookeyname'
    reservation.instances = [instance]
    (flexmock(EC2Connection)
      .should_receive('get_all_instances')
      .and_return([])
      .and_return([reservation]))
    (flexmock(EC2Connection)
      .should_receive('terminate_instances')
      .and_return([instance]))
    (flexmock(EC2Connection)
     .should_receive('request_spot_instances')
     .and_return())

    (flexmock(utils)
      .should_receive('get_secret')
      .and_return('secret'))
    (flexmock(utils)
      .should_receive('sleep')
      .and_return())
    (flexmock(utils)
      .should_receive('get_random_alphanumeric')
      .and_return('0000000000'))
    (flexmock(utils)
      .should_receive('write_key_file')
      .and_return())

  def tearDown(self):
    (flexmock(utils)
     .should_receive('get_secret')
     .reset())
    (flexmock(utils)
     .should_receive('sleep')
     .reset())
    (flexmock(utils)
     .should_receive('get_random_alphanumeric')
     .reset())

