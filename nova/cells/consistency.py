import time
import datetime
import random

from oslo.config import cfg

from nova.cells import rpcapi as cells_rpcapi
from nova.openstack.common import log as logging
from nova.openstack.common import timeutils
from nova import exception

LOG = logging.getLogger('nova.cells.consistency')


cell_consistency_opts = [
        cfg.IntOpt("heal_update_interval",
                default=10,
                help="Number of seconds between cell healing updates"),
        cfg.IntOpt("heal_updated_at_threshold",
                default=0,
                help="Number of seconds after a healable was updated "
                        "or deleted to continue to update cells"),
        cfg.IntOpt("heal_update_number",
                default=10,
                help="Number of healables to update per periodic task run")
]


LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(cell_consistency_opts, group='cells')


class ConsistencyHandler(object):

    def __init__(self, db):
        self.db = db
        self.cells_rpcapi = cells_rpcapi.CellsAPI()
        self.update_interval = CONF.cells.heal_update_interval
        self.update_threshold = CONF.cells.heal_updated_at_threshold
        self.update_number = CONF.cells.heal_update_number
        self.entries_to_heal = iter([])
        self.get_entries_filtered = None
        self.updated_list = False
        self.last_heal_time = 0
        self.model_name = 'entry'
        self.model_name_plural = 'entries'

    def reset(self):
        self.updated_list = False

    def _get_entries_to_heal(self, ctxt, updated_since=None,
            project_id=None, deleted=True, shuffle=False):

        filters = {}
        if updated_since is not None:
            filters['changes-since'] = updated_since
        if project_id is not None:
            filters['project_id'] = project_id
        if not deleted:
            filters['deleted'] = False
        entries = self.get_entries_filtered(ctxt, filters, 'deleted', 'asc')
        if shuffle:
            random.shuffle(entries)

        for entry in entries:
            yield entry

    def _get_next_entry(self, ctxt):
        try:
            entry = self.entries_to_heal.next()
        except StopIteration:
            if self.updated_list:
                return
            threshold = self.update_threshold
            updated_since = None
            if threshold > 0:
                updated_since = timeutils.utcnow() - datetime.timedelta(
                        seconds=threshold)
            self.entries_to_heal = self._get_entries_to_heal(
                    ctxt, updated_since=updated_since, shuffle=True)
            self.updated_list = True
            try:
                entry = self.entries_to_heal.next()
            except StopIteration:
                return
        return entry

    def _is_time_to_heal(self, curr_time):
        if not self.update_interval:
            return False
        if self.last_heal_time + self.update_interval > curr_time:
            return False
        return True

    def _send_destroy(self, ctxt, entry):
        msg = _('Healing process attempted to send delete message from '
                'base ConsistencyManager class, which is not supported')
        LOG.error(msg)

    def _send_create(self, ctxt, entry):
        msg = _('Healing process attempted to send create message from '
                'base ConsistencyManager class, which is not supported')
        LOG.error(msg)

    def _sync_entry(self, ctxt, entry):
        """broadcast an instance_update or instance_destroy message up to
        parent cells.
        """
        if entry['deleted']:
            LOG.debug(_("Sending message to delete %s" % (self.model_name_plural)))
            self._send_destroy(ctxt, entry)
        else:
            LOG.debug(_("Sending message to create %s" % (self.model_name_plural)))
            self._send_create(ctxt, entry)

    def set_last_heal_time(self, last_heal_time):
        self.last_heal_time = last_heal_time

    def _current_time(self):
        return time.time()

    def heal_entries(self, ctxt):
        LOG.debug(_("Checking if it's time to sync %s") % self.model_name_plural)
        curr_time = self._current_time()
        if not self._is_time_to_heal(curr_time):
            return
        self.set_last_heal_time(curr_time)
        self.reset()
        self._heal_entries(ctxt)

    def _heal_entries(self, ctxt):
        num_entries = self.update_number
        LOG.info(_("Synching %s %s" % (num_entries, self.model_name_plural)))
        for i in xrange(num_entries):
            while True:
                # Yield to other greenthreads
                time.sleep(0)
                entry = self._get_next_entry(ctxt)
                if not entry:
                    LOG.info("No more %s to sync" % (self.model_name_plural))
                    return
                self._sync_entry(ctxt, entry)
                #TODO why is this break here?
                break


class GroupConsistencyHandler(ConsistencyHandler):
    def __init__(self, *args, **kwargs):
        super(GroupConsistencyHandler, self).__init__(*args, **kwargs)
        self.get_entries_filtered = self.db.security_group_get_all_by_filters
        self.model_name = 'group'
        self.model_name_plural = 'groups'

    def _send_create(self, ctxt, group):
        self.cells_rpcapi.security_group_create(ctxt, group)

    def _send_destroy(self, ctxt, group):
        # A security group may have been deleted then created again
        # with the same name/project id. We only want to delete it if
        # it's the most recent occurence and it's marked deleted.
        filters = {'project_id': group['project_id'],
                   'name': group['name'],
                   'deleted': False}
        groups = self.db.security_group_get_all_by_filters(ctxt, filters,
                                                      'created_at', 'desc')
        if groups:
            return
        self.cells_rpcapi.security_group_destroy(ctxt, group)


class RuleConsistencyHandler(ConsistencyHandler):
    def __init__(self, *args, **kwargs):
        super(RuleConsistencyHandler, self).__init__(*args, **kwargs)
        self.get_entries_filtered = self.db.security_group_rule_get_all_by_filters
        self.model_name = 'rule'
        self.model_name_plural = 'rules'

    def _send_create(self, ctxt, rule):
        try:
            group = self.db.security_group_get(ctxt, rule['parent_group_id'])
        except exception.SecurityGroupNotFound:
            # Rule exists but group deleted, do nothing.
            return
        self.cells_rpcapi.security_group_rule_create(ctxt, group, rule)

    def _send_destroy(self, ctxt, rule):
        filters = {'to_port': rule['to_port'],
                   'from_port': rule['from_port'],
                   'parent_group_id': rule['parent_group_id'],
                   'protocol': rule['protocol']}
        if rule['cidr']:
            filters['cidr'] = rule['cidr']
        if rule['group_id']:
            filters['group_id'] = rule['group_id']
        filters['deleted'] = False
        rules = self.db.security_group_rule_get_all_by_filters(ctxt,
                    filters, 'created_at', 'desc')
        if rules:
            return
        try:
            group = self.db.security_group_get(ctxt, rule['parent_group_id'])
        except exception.SecurityGroupNotFound:
            # Group is deleted so don't care
            return
        self.cells_rpcapi.security_group_rule_destroy(ctxt, group, rule)


class InstanceAssociationConsistencyHandler(ConsistencyHandler):

    def __init__(self, *args, **kwargs):
        super(InstanceAssociationConsistencyHandler, self).__init__(*args, **kwargs)
        self.get_entries_filtered = self.db.security_group_instance_association_get_all_by_filters
        self.model_name = 'instance association'
        self.model_name_plural= 'instance associations'

    def _send_create(self, ctxt, instance_association):
        LOG.info("Sending create to %s" % instance_association.iteritems())
        self.cells_rpcapi.instance_add_security_group(
            ctxt,
            instance_association['instance_uuid'],
            instance_association['security_group_id'],
        )

    def _send_destroy(self, ctxt, instance_association):
        LOG.info("Sending remove to %s" % instance_association['security_group_id'])
        self.cells_rpcapi.instance_remove_security_group(
            ctxt,
            instance_association['instance_uuid'],
            instance_association['security_group_id'],
        )


class MappingConsistencyHandler(ConsistencyHandler):

    def __init__(self, *args, **kwargs):
        super(MappingConsistencyHandler, self).__init__(*args, **kwargs)
        self.create_function = None
        self.destroy_function = None

    def _send_create(self, ctxt, entry):
        fn = getattr(self.cells_rpcapi, self.create_function, None)
        fn(ctxt, entry['uuid'], entry['id'])
        LOG.debug(_('Sent broadcast message down to create %(model_name)s '
                    'with id=%(id)s and uuid=%(uuid)s.') % (
                        dict(model_name=self.model_name,
                             id=entry['id'], uuid=entry['uuid'])))

    def _send_destroy(self, ctxt, entry):
        msg = _('Healing process attempted to delete an %s entry, '
                'which is not supported.') % self.model_name
        LOG.error(msg)


class InstanceIDMappingConsistencyHandler(MappingConsistencyHandler):

    def __init__(self, *args, **kwargs):
        super(InstanceIDMappingConsistencyHandler, self).__init__(*args,
                                                                  **kwargs)
        self.get_entries_filtered = self.db.ec2_instance_get_all_by_filters
        self.model_name = 'instance id mapping'
        self.model_name_plural = 'instance id mappings'
        self.create_function = 'ec2_instance_create'


class S3ImageConsistencyHandler(MappingConsistencyHandler):

    def __init__(self, *args, **kwargs):
        super(S3ImageConsistencyHandler, self).__init__(*args, **kwargs)
        self.get_entries_filtered = self.db.s3_image_get_all_by_filters
        self.model_name = 's3 image'
        self.model_name_plural = 's3 images'
        self.create_function = 's3_image_create'


class VolumeIDMappingConsistencyHandler(MappingConsistencyHandler):

    def __init__(self, *args, **kwargs):
        super(VolumeIDMappingConsistencyHandler, self).__init__(*args,
                                                                **kwargs)
        self.get_entries_filtered = self.db.ec2_volume_get_all_by_filters
        self.model_name = 'volume id mapping'
        self.model_name_plural = 'volume id mappings'
        self.create_function = 'ec2_volume_create'
