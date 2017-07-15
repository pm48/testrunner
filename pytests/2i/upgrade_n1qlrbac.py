from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from pytests.security.rbac_base import RbacBase
from remote.remote_util import RemoteMachineShellConnection
from upgrade_2i import UpgradeSecondaryIndex
from pytests.tuqquery.n1ql_rbac_2 import RbacN1QL
import logging

QUERY_TEMPLATE = "SELECT {0} FROM %s "
log = logging.getLogger(__name__)

class UpgradeN1QLRBAC(UpgradeSecondaryIndex,RbacN1QL):
    def setUp(self):
        super(UpgradeN1QLRBAC, self).setUp()
        self.dataset = self.input.param("dataset", "default")
        self.sasl_buckets = 1
        self.num_plasma_buckets = self.input.param("standard_buckets", 1)
        self.shell = RemoteMachineShellConnection(self.master)



    def tearDown(self):
        self.upgrade_servers = self.servers
        super(UpgradeN1QLRBAC, self).tearDown()


    # This test is run with sasl bucket.Secondary and primay indexes are created before upgrade
    # After upgrade we make sure that queries can use these indexes for sasl and non sasl buckets.
    # We also use pre-upgrade users for the query with inedxes.
    def test_offline_upgrade_with_rbac(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self.test_offline_upgrade()
        self.sleep(10)
        self._create_standard_buckets(self.master, 1)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('post-upgrade', 'postupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # create secondary index on standard_bucket
        self.query = 'create index idx2 on {0}(meta().id)'.format('standard_bucket0')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        # verify number of users after upgrade
        self.assertTrue(actual_result['metrics']['resultCount'] == 8)
        self.create_users(users=[{'id': 'john',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("admin",'john')
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 9)

        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin",'johnClusterAdmin')
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)

        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'johnClusterAdmin'))
            # use pre-upgrade users
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john_admin','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_admin'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john_cluster','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_cluster'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('mary','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'mary'))



    # This test creates different users with different query permissions and validates the specific
    # permissions after upgrade.We use pre-upgrade users for different queries and then change permissions on them and verify
    # various queries accordingly. We also change permissions on new users and verify queries accordingly.
    def test_offline_upgrade_with_new_users(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self.test_offline_upgrade()
        self._create_standard_buckets(self.master, 1)
        # create secondary index on standard_bucket
        self.query = 'create index idx2 on {0}(meta().id)'.format('standard_bucket0')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('post-upgrade', 'postupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 15)
        self.check_permissions_helper()
        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin",'johnClusterAdmin')
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['status'] == 'success')
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin','password', self.master.ip, 'bucket0',self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('bucket0', 'johnClusterAdmin'))
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from my_user_info'".format('johnClusterAdmin','password', self.master.ip, 'bucket0',self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('my_user_info', 'johnClusterAdmin'))
        self.use_pre_upgrade_users_post_upgrade()
        self.change_permissions_and_verify_pre_upgrade_users()
        self.change_permissions_and_verify_new_users()


    # This test does the online upgrade ,validates the specific
    # permissions after upgrade and verifies the number of users created are correct.
    # It also verifies the queries use the correct index for sasl buckets after online upgrade.
    def test_online_upgrade_with_rbac(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
         # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self.test_online_upgrade()
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        #verify number of users after upgrade
        self.assertTrue(actual_result['resultCount'] == 2)

        self._create_standard_buckets(self.master, 1)
        # create secondary index on standard_bucket
        self.query = 'create index idx2 on {0}(meta().id)'.format('standard_bucket0')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)
        self.check_permissions_helper()

        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john_bucket_admin','password', self.master.ip, 'bucket0',self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('bucket0', 'john_bucket_admin'))
        self.use_pre_upgrade_users_post_upgrade()
        self.change_permissions_and_verify_pre_upgrade_users()
        self.change_permissions_and_verify_new_users()


    # This test does online upgrade and checks various system catalog users
    # It might fail based on implementation details from dev.
    def test_online_upgrade_with_system_catalog(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self.query = 'create primary index on {0}'.format('bucket0')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        self.test_online_upgrade()
        self.create_and_verify_system_catalog_users_helper()
        self.check_system_catalog_helper()


    # This test does offline upgrade and checks various system catalog users
    # It might fail based on implementation details from dev.
    def test_offline_upgrade_with_system_catalog(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self.query = 'create primary index on {0}'.format('bucket0')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        self.test_offline_upgrade()
        self.create_and_verify_system_catalog_users_helper()
        self.check_system_catalog_helper()

    # This test does the online upgrade with swap rebalance.It validates the specific
    # permissions after upgrade and verifies the number of users created are correct.
    def test_online_upgrade_swap_rebalance_with_rabc(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.create_users_before_upgrade_non_ldap()
        self.test_online_upgrade_swap_rebalance()
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 2)
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self.create_users(users=[{'id': 'john',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("admin",'john')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)

        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 3)
        self._create_standard_buckets(self.master, 1)
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)
        self.check_permissions_helper()
        self.use_pre_upgrade_users_post_upgrade()
        self.change_permissions_and_verify_pre_upgrade_users()
        self.change_permissions_and_verify_new_users()




    # This test does the online upgrade with mixed node cluster.It validates the specific
    # permissions after upgrade and verifies the number of users created are correct.
    def test_online_upgrade_with_mixed_mode_cluster_with_rbac(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.create_users_before_upgrade_non_ldap()
        self.test_online_upgrade_with_mixed_mode_cluster()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 2)
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, 1)
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)
        self.check_permissions_helper()
        self.use_pre_upgrade_users_post_upgrade()
        self.change_permissions_and_verify_pre_upgrade_users()
        self.change_permissions_and_verify_new_users()



    # This test does the online upgrade with two query nodes and verifies prepared statements work as expected
    # with admin user.This test is specific to prepared statements for rbac users after upgrade.
    def test_online_upgrade_with_two_query_nodes_with_rbac(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.test_online_upgrade_path_with_rebalance()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 2)
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self.create_users(users=[{'id': 'john',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("admin",'john')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 3)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement=PREPARE SELECT * from %s LIMIT 10'"%\
                (self.curl_path,'john','password', self.master.ip, self.buckets[0].name)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to prepare select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        log.info("Prepare query executed successfully")
        self.create_and_verify_system_catalog_users_helper()


    # This test does offline upgrade and tests if users created before upgrade are working correctly after upgrade.
    # The users created before upgrade are verified for functionality in verify_pre_upgrade_users_permissions_helper.
    # Permissions for the users created before upgrade are changed after upgrade to new query based permissions in
    # change_pre_upgrade_users_permissions.
    def test_offline_upgrade_check_ldap_users_before_upgrade(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        # create ldap users before upgrade
        self.create_ldap_auth_helper()
        self.test_offline_upgrade()
        self.sleep(20)
        self._create_standard_buckets(self.master, 1)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 6)
        self.verify_pre_upgrade_users_permissions_helper()
        self.change_and_verify_pre_upgrade_ldap_users_permissions()
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 13)
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()


    # This test does online upgrade and tests if users created before upgrade are working correctly after upgrade.
    # The users created before upgrade are verified for functionality in verify_pre_upgrade_users_permissions_helper.
    # Permissions for the users created before upgrade are changed after upgrade to new query based permissions in
    # change_pre_upgrade_users_permssions.
    def test_online_upgrade_check_ldap_users_before_upgrade(self):
        self.bucket_size = 100
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        for bucket in self.buckets:
            self.query = 'create primary index on {0}'.format(bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        # create ldap users before upgrade
        self.create_ldap_auth_helper()
        self.test_online_upgrade()
        self.sleep(20)
        self._create_standard_buckets(self.master, 1)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['resultCount'] == 6)
        self.verify_pre_upgrade_users_permissions_helper()
        self.change_and_verify_pre_upgrade_ldap_users_permissions()
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 13)
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()



    def query_select_insert_update_delete_helper(self):
        self.create_users(users=[{'id': 'john_insert',
                                           'name': 'johnInsert',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_update',
                                           'name': 'johnUpdate',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_delete',
                                           'name': 'johnDelete',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select',
                                           'name': 'johnSelect',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select2',
                                           'name': 'johnSelect2',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_rep',
                                           'name': 'johnRep',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_bucket_admin',
                                           'name': 'johnBucketAdmin',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("replication_admin",'john_rep')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.query = "GRANT {0} on standard_bucket0 to {1}".format("query_select",'john_select2')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        for bucket in self.buckets:
            self.query = "GRANT {0} on {2} to {1}".format("query_insert",'john_insert',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_update",'john_update',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_delete",'john_delete',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_select",'john_select',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("bucket_admin",'john_bucket_admin',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)



    def check_permissions_helper(self):
      for bucket in self.buckets:
         cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'"%\
                (self.curl_path,'john_insert', 'password', self.master.ip, bucket.name)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(bucket.name, 'johnInsert'))
         log.info("Query executed successfully")
         old_name = "employee-14"
         new_name = "employee-14-2"
         cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format('john_update', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                        format(bucket.name, 'johnUpdate'))
         log.info("Query executed successfully")
         del_name = "employee-14"
         cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('john_delete', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'john_delete'))
         log.info("Query executed successfully")
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('john_select', 'password', self.master.ip,'bucket0',self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_select'))
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('john_select2', 'password', self.master.ip,'standard_bucket0',self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_select2'))


    #This function is separated from check_system_catalog_helper since its the first step for system catalog users
    # and should works well for all upgrades.
    def create_and_verify_system_catalog_users_helper(self):
        self.create_users(users=[{'id': 'john_system',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("query_system_catalog",'john_system')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:namespaces'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:datastores'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:indexes'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:completed_requests'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:active_requests'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:prepareds'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:my_user_info'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))





    #These test might fail for now as system catalog tables are not fully implemented based on query PM's doc.
    def check_system_catalog_helper(self):
        self.system_catalog_helper_delete()
        self.system_catalog_helper_update()
        self.system_catalog_helper_insert()
        self.system_catalog_helper_select()




    def change_and_verify_pre_upgrade_ldap_users_permissions(self):
        for bucket in self.buckets:
            # change permission of john_bucketadmin1 and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,'john_bucketadmin1')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_bucketadmin1', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_bucketadmin1'))

            # change permission of john_bucketadminAll and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_insert",bucket.name,'john_bucketadmin1')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=INSERT INTO {3} (KEY, VALUE) " \
                  "VALUES(\"1\", { \"value1\": \"one1\" })'".format('john_bucketadminAll', 'password',
                                                                    self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'john_bucketadmin1'))

            # change permission of cluster_user and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_update",bucket.name,'cluster_user')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('cluster_user', 'password',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'cluster_user'))

            # change permission of read_user and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'read_user')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('read_user', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'read_user'))
            log.info("Query executed successfully")

            # change permission of cadmin user and verify its able to execute the correct query.
            self.query = "GRANT {0} to {1}".format("query_system_catalog",'cadmin')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('cadmin','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from system:keyspaces as user {1}".
                        format('cadmin'))


    # Helper function for creating ldap users pre-upgrade
    def create_ldap_auth_helper(self):
        # not able to create bucket admin on passwordless bucket pre upgrade
        users = [{'id': 'john_bucketadmin1', 'name': 'john_bucketadmin1', 'password': 'password'},
                 {'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll', 'password': 'password'},
                 {'id': 'cluster_user','name':'cluster_user','password':'password'},
                 {'id': 'read_user','name':'read_user','password':'password'},
                 {'id': 'cadmin','name':'cadmin','password':'password'},]
        RbacBase().create_user_source(users, 'ldap', self.master)
        rolelist = [{'id': 'john_bucketadmin1', 'name': 'john_bucketadmin','roles': 'bucket_admin[bucket0]'},
                    {'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll','roles': 'bucket_admin[*]'},
                    {'id': 'cluster_user', 'name': 'cluster_user','roles': 'cluster_admin'},
                    {'id': 'read_user', 'name': 'read_user','roles': 'ro_admin'},
                    {'id': 'cadmin', 'name': 'cadmin','roles': 'admin'}]
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'ldap')


    def verify_pre_upgrade_users_permissions_helper(self):
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                    format('john_bucketadmin1', 'password', self.master.ip,'bucket0',self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format('bucket0', 'john_select'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                    format('john_bucketadmin2', 'password', self.master.ip,'standard_bucket0',self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format('standard_bucket0', 'john_select2'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                    format('cadmin', 'password', self.master.ip,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format('system:keyspaces', 'cadmin'))

            for bucket in self.buckets:
             cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
                  "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'"%\
                    (self.curl_path,'john_bucketadminAll', 'password', self.master.ip, self.buckets[0].name)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'john_bucketadminAll'))
             log.info("Query executed successfully")
             old_name = "employee-14"
             new_name = "employee-14-2"
             cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                    format('john_bucketadminAll', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                            format(bucket.name, 'john_bucketadminAll'))
             log.info("Query executed successfully")
             del_name = "employee-14"
             cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                    format('john_bucketadminAll', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                            format(bucket.name, 'john_bucketadminAll'))
             log.info("Query executed successfully")
             cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('cluster_user', 'password', self.master.ip,bucket.name,self.curl_path)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'cluster_user'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('read_user', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("error" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'read_user'))


    def use_pre_upgrade_users_post_upgrade(self):
        for bucket in self.buckets:
         cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'"%\
                (self.curl_path,'john_admin', 'password', self.master.ip, bucket.name)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(bucket.name, 'johnInsert'))
         log.info("Query executed successfully")
         old_name = "employee-14"
         new_name = "employee-14-2"
         cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format('john_cluster', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                        format(bucket.name, 'johnUpdate'))
         log.info("Query executed successfully")
         del_name = "employee-14"
         cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=select * from {3} WHERE name = '{4}' limit 2'".\
                format('bob', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'bob'))
         log.info("Query executed successfully")
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('mary', 'password', self.master.ip,'bucket0',self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'mary'))
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('ro_non_ldap', 'password', self.master.ip,bucket.name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.assertTrue(any("error" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'ro_non_ldap'))

    def change_permissions_and_verify_pre_upgrade_users(self):
        for bucket in self.buckets:
            # change permission of john_cluster and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,'john_cluster')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_cluster', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_cluster'))

            # change permission of mary and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_insert",bucket.name,'mary')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=INSERT INTO {3} (KEY, VALUE) " \
                  "VALUES(\"1\", { \"value1\": \"one1\" })'".format('mary', 'password',
                                                                    self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'mary'))

            # change permission of ro_non_ldap and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_update",bucket.name,'ro_non_ldap')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('ro_non_ldap', 'password',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'ro_non_ldap'))

            # change permission of john_admin and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'john_admin')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('john_admin', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'john_admin'))
            log.info("Query executed successfully")

            # change permission of bob user and verify its able to execute the correct query.
            self.query = "GRANT {0} to {1}".format("query_system_catalog",'bob')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('bob','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from system:keyspaces as user {1}".
                        format('bob'))

    def change_permissions_and_verify_new_upgrade_users(self):
        for bucket in self.buckets:
            # change permission of john_insert and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("bucket_admin",bucket.name,'john_insert')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_insert', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_insert'))

            # change permission of john_update and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_insert",bucket.name,'john_update')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=INSERT INTO {3} (KEY, VALUE) " \
                  "VALUES(\"1\", { \"value1\": \"one1\" })'".format('mary', 'password',
                                                                    self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'john_update'))

            # change permission of john_select and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("cluster_admin",bucket.name,'john_select')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('john_select', 'password',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'john_select'))

            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_select', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_select'))

            # change permission of john_select2 and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'john_select2')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('john_select2', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'john_select2'))
            log.info("Query executed successfully")

            # change permission of john_delete and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,'john_delete')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_delete', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_delete'))

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users,'builtin',self.master)
        log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def create_users_before_upgrade_non_ldap(self):
        log.info("create a read only user account")
        self.shell.execute_command("%scouchbase-cli "
                                              "user-manage -c %s:8091 --set "
                                              "--ro-username=%s "
                                            "--ro-password=readonlypassword "
                                              "-u Administrator -p %s "
                                      % (self.path, self.master.ip,
                                                  'ro_non_ldap', 'password'))
        log.info("create a bucket admin on bucket0 user account")
        self.shell.execute_command("%scouchbase-cli "
                                              "admin-role-manage -c %s:8091 --set-users=bob "
                                              "--set-names=Bob "
                                              "--roles=bucket_admin[bucket0] "
                                              "-u Administrator -p %s "
                                      % (self.path, self.master.ip,
                                                'password'))

        log.info("create a bucket admin on all buckets user account")
        self.shell.execute_command("%scouchbase-cli "
                                              "admin-role-manage -c %s:8091 --set-users=mary "
                                              "--set-names=Mary "
                                              "--roles=bucket_admin[*] "
                                              "-u Administrator -p %s "
                                      % (self.path, self.master.ip,
                                                  'password'))

        log.info("create a cluster admin user account")
        self.shell.execute_command("%scouchbase-cli "
                                              "admin-role-manage -c %s:8091 --set-users=john_cluster "
                                              "--set-names=john_cluster "
                                              "--roles=cluster_admin "
                                              "-u Administrator -p %s "
                                     % (self.path, self.master.ip,
                                                  'password'))

        log.info("create a admin user account")
        self.shell.execute_command("%scouchbase-cli "
                                              "admin-role-manage -c %s:8091 --set-users=john_admin "
                                              "--set-names=john_admin "
                                              "--roles=admin "
                                              "-u Administrator -p %s "
                                     % (self.path, self.master.ip,
                                                  'password'))
