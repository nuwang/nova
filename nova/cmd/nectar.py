import socket

from nova import context
from nova.conductor import rpcapi as conductor_rpcapi
from nova.objects import block_device, instance as instance_obj


# Decorators for actions
def args(*args, **kwargs):
    def _decorator(func):
        func.__dict__.setdefault('args', []).insert(0, (args, kwargs))
        return func
    return _decorator


class NectarCommands(object):
    """Class for NeCTAR commands."""

    @args('--instance', metavar='<instance>', help='Instance')
    @args('--volume', metavar='<volume>', help='Volume')
    def remove_bdm(self, instance, volume):
        ctx = context.get_admin_context()
        c = conductor_rpcapi.ConductorAPI()
        instance = instance_obj.Instance.get_by_uuid(ctx, instance)

        if instance['host'] != socket.gethostname():
            print("Instance not running on this host")
            return
        bdms = c.block_device_mapping_get_all_by_instance(ctx, instance)
        for bdm in bdms:
            if bdm['volume_id'] == volume:
                b = block_device.BlockDeviceMapping.get_by_volume_id(
                    ctx, volume, instance_uuid=instance['uuid'])

                print("Deleting BDM with ID %s" % b.id)
                b.destroy()
